package com.btoddb.flume.avroclient;

import java.io.FileReader;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.btoddb.flume.sinks.cassandra.FlumeLogEvent;
import com.btoddb.flume.sinks.cassandra.JmxStatsHelper;

public class AvroTestClient implements AvroClientTestMXBean {
    private static final Logger logger = LoggerFactory.getLogger(AvroTestClient.class);
    
    private static final String MBEAN_NAME_ROOT = "com.btoddb.flume.avroclient:type=test-client";

    private static final String STAT_REQUESTS = "reqs";

    private String host;
    private int port;
    private int batchSize;
    private int iterations;
    private int delay;

    private MBeanServer mbs;
    private JmxStatsHelper stats;

    public AvroTestClient(String[] args) {
        init(args);
    }

    private void init(String[] args) {
        if (5 != args.length) {
            showUsage();
            System.exit(-1);
        }

        host = args[0];
        port = Integer.parseInt(args[1]);
        batchSize = Integer.parseInt(args[2]);
        iterations = Integer.parseInt(args[3]);
        delay = Integer.parseInt(args[4]);

        if (null == stats) {
            stats = new JmxStatsHelper(5 * 1000);
        }

        mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME_ROOT));
        }
        catch (Throwable e) {
            logger.error("exception while registering me as mbean, " + MBEAN_NAME_ROOT, e);
        }
    }

    private void showUsage() {
        System.out.println();
        System.out.println("usage: AvroTestclient <host> <port> <batch-size> <iterations> <delay-in-millis>");
        System.out.println();
    }

    public void start() throws IOException {

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
        System.out.println("sample data (" + data.length() + ") : " + data);
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

        try {
            for (int i = 0; i < iterations; i++) {
                if (0 == i % 100) {
                    System.out.println("iteration : " + i + ", " + stats.getStat(STAT_REQUESTS).getCountPerSecond()
                            + " reqs/sec");
                }
                Event event = EventBuilder.withBody(("test event : " + i + " : " + data).getBytes(), headerMap);
                long start = System.nanoTime();
                rpcClient.append(event);
                stats.update(STAT_REQUESTS, 1, (System.nanoTime() - start) / 1000);
                try {
                    Thread.sleep(delay);
                }
                catch (InterruptedException e) {
                    // ignore
                    Thread.interrupted();
                }
            }
        }
        catch (EventDeliveryException e) {
            e.printStackTrace();
        }

        rpcClient.close();
    }

    public static void main(String[] args) throws Exception {
        AvroTestClient client = new AvroTestClient(args);
        client.start();
    }

    @Override
    public int getRequestsPerSecond() {
        return stats.getStat(STAT_REQUESTS).getCountPerSecond();
    }

    @Override
    public int getRequestAvgInMicros() {
        return stats.getStat(STAT_REQUESTS).getAverageAmount();
    }

    @Override
    public int getDelay() {
        return delay;
    }

    @Override
    public void setDelay(int delayInMillis) {
        this.delay = delayInMillis;
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public int getIterations() {
        return iterations;
    }

    @Override
    public void setIterations(int iterations) {
        this.iterations = iterations;
    }
}
