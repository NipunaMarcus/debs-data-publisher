/*
*  Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.debs.datapublisher;

import org.wso2.carbon.databridge.agent.AgentHolder;
import org.wso2.carbon.databridge.agent.DataPublisher;
import org.wso2.carbon.databridge.commons.Event;
import org.wso2.carbon.databridge.commons.utils.DataBridgeCommonsUtils;

import java.io.*;
import java.net.*;
import java.util.Enumeration;

public class NewDEBSAgent {

    private static final String DEBS_STREAM = "DEBS_DATA";
    private static final String VERSION = "1.0.0";
    private static int count = 10000;

    public static void main(String[] args) throws Exception {

        String server = args[0];
        String username = args[1];
        String password = args[2];
        String file = args[3];
        count = Integer.parseInt(args[4]);

        System.out.println("Initialized DEBS data publisher on " + server + " with " + count + " records to write");

        System.setProperty("javax.net.ssl.trustStore", "./src/main/resources/truststore/client-truststore.jks");
        System.setProperty("javax.net.ssl.trustStorePassword", "wso2carbon");

        AgentHolder.setConfigPath(getDataAgentConfigPath());

        String connParams[] = server.split(":");

        int receiverPort = Integer.parseInt(connParams[1]);
        int securePort = receiverPort + 100;

        String url = "tcp://" + connParams[0] + ":" + receiverPort;
        String authURL = "ssl://" + connParams[0] + ":" + securePort;

        DataPublisher dataPublisher = new DataPublisher("Thrift", url, authURL, username, password);

        String streamId = DataBridgeCommonsUtils.generateStreamId(DEBS_STREAM, VERSION);
        publishEvents(dataPublisher, streamId, file);

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            // do nothing
        }
        dataPublisher.shutdown();
    }

    public static String getDataAgentConfigPath() {
        File filePath = new File("src" + File.separator + "main" + File.separator + "resources");
        if (!filePath.exists()) {
            filePath = new File("test" + File.separator + "resources");
        }
        if (!filePath.exists()) {
            filePath = new File("resources");
        }
        if (!filePath.exists()) {
            filePath = new File("test" + File.separator + "resources");
        }
        return filePath.getAbsolutePath() + File.separator + "data-agent-conf.xml";
    }

    private static void publishEvents(DataPublisher dataPublisher, String streamId, String file) throws FileNotFoundException {

        BufferedReader br;
        FileReader fr;
        String host;

        if (new File(file).exists()) {
            try {
                if (getLocalAddress() != null) {
                    host = getLocalAddress().getHostAddress();
                } else {
                    host = "localhost"; // Defaults to localhost
                }
                fr = new FileReader(file);
                br = new BufferedReader(fr);
                String line;
                int ctr = 0;

                while ((line = br.readLine()) != null) {
                    String[] data = line.split(",");
                    Object[] payload = new Object[]{
                            data[0], Float.parseFloat(data[2]), Boolean.parseBoolean(data[3]),
                            Integer.parseInt(data[4]), Integer.parseInt(data[5]), Integer.parseInt(data[6])
                    };

                    //constructor used: Event(streamID, timeStamp, metaArray, correlationArray, payloadArray)
                    Event event = new Event(streamId, System.currentTimeMillis(), new Object[]{host}, null, payload);

                    dataPublisher.publish(event);
                    ctr++;
                    if (count > 0) {
                        if (ctr == count) {
                            break;
                        }
                    }
                }

                br.close();
                fr.close();

                System.out.println("Published " + ctr + " events.");

            } catch (NumberFormatException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            throw new FileNotFoundException("target file not found!");
        }
    }

    public static InetAddress getLocalAddress() throws SocketException, UnknownHostException {
        Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
        while (ifaces.hasMoreElements()) {
            NetworkInterface iface = ifaces.nextElement();
            Enumeration<InetAddress> addresses = iface.getInetAddresses();

            while (addresses.hasMoreElements()) {
                InetAddress addr = addresses.nextElement();
                if (addr instanceof Inet4Address && !addr.isLoopbackAddress()) {
                    return addr;
                }
            }
        }
        return InetAddress.getLocalHost();
    }

}
