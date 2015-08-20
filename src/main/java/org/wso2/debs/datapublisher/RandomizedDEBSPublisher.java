package org.wso2.debs.datapublisher;


import org.wso2.carbon.databridge.agent.thrift.Agent;
import org.wso2.carbon.databridge.agent.thrift.DataPublisher;
import org.wso2.carbon.databridge.agent.thrift.conf.AgentConfiguration;
import org.wso2.carbon.databridge.agent.thrift.exception.AgentException;
import org.wso2.carbon.databridge.commons.Event;
import org.wso2.carbon.databridge.commons.exception.NoStreamDefinitionExistException;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.Random;
import java.util.UUID;

public class RandomizedDEBSPublisher {

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
        count = Integer.parseInt(args[3]);

        String streamId;

        System.out.println("Initialized DEBS data publisher on " + server + " with " + count + " records to write");

        System.setProperty("javax.net.ssl.trustStore", "./src/main/resources/truststore/client-truststore.jks");
        System.setProperty("javax.net.ssl.trustStorePassword", "wso2carbon");

        AgentConfiguration agentConfiguration = new AgentConfiguration();
        agentConfiguration.setBufferedEventsSize(50000);

        Agent agent = new Agent(agentConfiguration);

        DataPublisher dataPublisher = new DataPublisher("tcp://" + server, username, password, agent);

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

        try {
            streamId = dataPublisher.findStream(DEBS_DATA_STREAM, VERSION);
            //Stream already defined

        } catch (NoStreamDefinitionExistException e) {
            streamId = dataPublisher.defineStream(definition);
            System.out.println("Defining new stream for " + DEBS_DATA_STREAM);
        }

        if (!streamId.isEmpty()) {
            publishEvents(dataPublisher, streamId);
        } else {
            System.out.println("No events published.");
        }

        dataPublisher.stop();
    }

    private static void publishEvents(DataPublisher dataPublisher, String streamId) {
        String host;
        Random rand = new Random();

        try {
            if (getLocalAddress() != null) {
                host = getLocalAddress().getHostAddress();
            } else {
                host = "localhost"; // Defaults to localhost
            }

            int ctr = 0;

            while (ctr < count) {

                String[] data = new String[7];
                data[0] = UUID.randomUUID().toString();
                data[1] = Long.toString(System.currentTimeMillis() - 100000);
                data[2] = Float.toString(rand.nextFloat() * 500);
                data[3] = Integer.toString(rand.nextInt(2));
                data[4] = Integer.toString(rand.nextInt(7));
                data[5] = Integer.toString(rand.nextInt(21));
                data[6] = Integer.toString(rand.nextInt(6));

                Object[] payload = new Object[]{
                        data[0], Long.parseLong(data[1]), Float.parseFloat(data[2]), Integer.parseInt(data[3]),
                        data[4], data[5], data[6]
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

            System.out.println("Published " + ctr + " events.");

        } catch (SocketException e) {
            e.printStackTrace();
        } catch (AgentException e) {
            e.printStackTrace();
        }

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