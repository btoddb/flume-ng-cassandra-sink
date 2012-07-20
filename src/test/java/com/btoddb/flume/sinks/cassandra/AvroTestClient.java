package com.btoddb.flume.sinks.cassandra;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

import com.btoddb.flume.sinks.cassandra.FlumeLogEvent;

public class AvroTestClient {

    public static void main(String[] args) {
        String host = "localhost";
        int port = 4141;
        int batchSize = 10;

        RpcClient rpcClient = RpcClientFactory.getDefaultInstance(host, port, batchSize);

        Map<String, String> headerMap = new HashMap<String, String>();
        headerMap.put(FlumeLogEvent.HEADER_SOURCE, "src1");


        try {
            for (int i = 0; i < 15; i++) {
                Event event = EventBuilder.withBody(("test event : " + i).getBytes(), headerMap);
                rpcClient.append(event);
            }
        }
        catch (EventDeliveryException e) {
            e.printStackTrace();
        }

        rpcClient.close();

    }
}
