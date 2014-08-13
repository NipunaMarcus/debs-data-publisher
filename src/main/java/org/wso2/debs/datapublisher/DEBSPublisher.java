package org.wso2.debs.datapublisher;


import org.wso2.carbon.databridge.agent.thrift.AsyncDataPublisher;
import org.wso2.carbon.databridge.agent.thrift.exception.AgentException;
import org.wso2.carbon.databridge.commons.Event;

import java.io.*;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public class DEBSPublisher {

    public static final String DEBS_DATA_STREAM = "debs_data";
    public static final String VERSION = "1.0.0";

    private static int count = 1000;

    /**
     * id – a unique identifier of the measurement [64 bit unsigned integer value]
     * timestamp – timestamp of measurement (number of seconds since January 1, 1970, 00:00:00 GMT) [32 bit unsigned integer value]
     * value – the measurement [32 bit floating point]
     * property – type of the measurement: 0 for work or 1 for load [boolean]
     * plug_id – a unique identifier (within a household) of the smart plug [32 bit unsigned integer value]
     * household_id – a unique identifier of a household (within a house) where the plug is located [32 bit unsigned integer value]
     * house_id – a unique identifier of a house where the household with the plug is located [32 bit unsigned integer value]
     */

    public static void main(String[] args) throws Exception {
        String server = args[0];
        String username = args[1];
        String password = args[2];
        String file = args[3];
        count = Integer.parseInt(args[4]);

        System.out.println("Initialized DEBS data publisher on " + server + " with " + count + " records to write");

        System.setProperty("javax.net.ssl.trustStore", "./src/main/resources/truststore/client-truststore.jks");
        System.setProperty("javax.net.ssl.trustStorePassword", "wso2carbon");

        AsyncDataPublisher dataPublisher = new AsyncDataPublisher("tcp://" + server, username, password);

        String definition = "{" +
                "  'name':'" + DEBS_DATA_STREAM + "'," +
                "  'version':'" + VERSION + "'," +
                "  'nickName': 'DEBSStream'," +
                "  'description': 'DEBS data'," +
                "  'metaData':[" +
                "          {'name':'publisher','type':'STRING'}" +
                "  ]," +
                "  'payloadData':[" +
                "          {'name':'id','type':'STRING'}," +
                "          {'name':'timestamp','type':'LONG'}," +
                "          {'name':'value','type':'FLOAT'}," +
                "          {'name':'property','type':'INT'}," +
                "          {'name':'plug_id','type':'STRING'}," +
                "          {'name':'household_id','type':'STRING'}," +
                "          {'name':'house_id','type':'STRING'}" +
                "  ]" +
                "}";

        if (!(dataPublisher.isStreamDefinitionAdded(DEBS_DATA_STREAM, VERSION))) {
            dataPublisher.addStreamDefinition(definition, DEBS_DATA_STREAM, VERSION);
        }

        publishEvents(dataPublisher, file);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw e;
        }

        dataPublisher.stop();
    }

    private static void publishEvents(AsyncDataPublisher asyncDataPublisher, String file) throws FileNotFoundException {

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
                            data[0], Long.parseLong(data[1]), Float.parseFloat(data[2]), Integer.parseInt(data[3]),
                            data[4], data[5], data[6]
                    };

                    Event event = eventObject(null, new Object[]{host}, payload);
                    asyncDataPublisher.publish(DEBS_DATA_STREAM, VERSION, event);
                    ctr++;
                    if(count > 0){
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
            } catch (AgentException e) {
                e.printStackTrace();
            }
        } else {
            throw new FileNotFoundException("target file not found!");
        }
    }


    private static Event eventObject(Object[] correlationData, Object[] metaData, Object[] payLoadData) {
        Event event = new Event();
        event.setCorrelationData(correlationData);
        event.setMetaData(metaData);
        event.setPayloadData(payLoadData);
        return event;
    }

    private static InetAddress getLocalAddress() throws SocketException {
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
        return null;
    }
}