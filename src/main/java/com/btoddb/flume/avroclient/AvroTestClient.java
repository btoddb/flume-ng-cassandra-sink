package com.btoddb.flume.avroclient;

import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

import com.btoddb.flume.sinks.cassandra.FlumeLogEvent;
import com.btoddb.flume.sinks.cassandra.JmxStatsHelper;

public class AvroTestClient {

    public static void main(String[] args) throws Exception {
        if (5 != args.length) {
            showUsage();
            return;
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        int batchSize = Integer.parseInt(args[2]);
        int iterations = Integer.parseInt(args[3]);
        int delay = Integer.parseInt(args[4]);

        //
        // get sample data
        //
        
        FileReader reader = new FileReader("flume-traffic-sample.txt");
        StringBuffer sb = new StringBuffer();
        while (reader.ready()) {
            sb.append((char) reader.read());
        }
        reader.close();
        String data = sb.toString();

        System.out.println();
        System.out.println( "sample data (" + data.length() + ") : " + data);
        System.out.println();
        
        //
        // start avro
        //
        
        RpcClient rpcClient = RpcClientFactory.getDefaultInstance(host, port, batchSize);

        //
        // setup client headers
        //
        
        Map<String, String> headerMap = new HashMap<String, String>();
        headerMap.put(FlumeLogEvent.HEADER_SOURCE, "src1");

        JmxStatsHelper stats = new JmxStatsHelper(5000);
        try {
            for (int i = 0; i < iterations; i++) {
                if (0 == i % 100) {
                    System.out.println("iteration : " + i + ", " + stats.getStat("reqs").getCountPerSecond() + " reqs/sec");
                }
                Event event = EventBuilder.withBody(("test event : " + i + " : " + data).getBytes(), headerMap);
                long start = System.nanoTime();
                rpcClient.append(event);
                stats.update("reqs", 1, (System.nanoTime()-start)/1000);
                Thread.sleep(delay);
            }
        }
        catch (EventDeliveryException e) {
            e.printStackTrace();
        }

        rpcClient.close();
    }

    private static void showUsage() {
        System.out.println();
        System.out.println("usage: AvroTestclient <host> <port> <batch-size> <iterations> <delay-in-millis>");
        System.out.println();
    }
}
