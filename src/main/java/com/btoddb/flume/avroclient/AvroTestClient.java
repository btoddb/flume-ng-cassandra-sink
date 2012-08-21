package com.btoddb.flume.avroclient;

import java.io.FileReader;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.btoddb.flume.interceptors.MicrosecondsSyncClockResolution;
import com.btoddb.flume.sinks.cassandra.FlumeLogEvent;
import com.btoddb.flume.sinks.cassandra.JmxStatsHelper;

public class AvroTestClient implements AvroClientTestMXBean {
    private static final Logger logger = LoggerFactory.getLogger(AvroTestClient.class);

    private static final String MBEAN_NAME_ROOT = "com.btoddb.flume.avroclient:type=test-client";

    private static final String STAT_REQUESTS = "reqs";

    private String host;
    private int port;
    private int avroBatchSize;
    private int clientBatchSize;
    private int iterations;
    private int delay;
    private int numThreads;
    private String sourcePrefix;

    private MBeanServer mbs;
    private JmxStatsHelper stats;
    private AtomicLong numProcessed = new AtomicLong();

    private ExecutorService execSrvc;
    private ArrayBlockingQueue<String> workQueue;
    private boolean stopProcessing = false;
    private long start = System.currentTimeMillis();
    private String clientIp;
    public int maxSourceTypeId = 100;

    public AvroTestClient(String[] args) {
        init(args);
    }

    private void init(String[] args) {
        if (8 != args.length) {
            showUsage();
            System.exit(-1);
        }

        host = args[0];
        port = Integer.parseInt(args[1]);
        avroBatchSize = Integer.parseInt(args[2]);
        clientBatchSize = Integer.parseInt(args[3]);
        iterations = Integer.parseInt(args[4]);
        delay = Integer.parseInt(args[5]);
        numThreads = Integer.parseInt(args[6]);
        sourcePrefix = args[7];

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

        execSrvc = new ThreadPoolExecutor(numThreads, numThreads, 1, TimeUnit.HOURS,
                new ArrayBlockingQueue<Runnable>(1));

        workQueue = new ArrayBlockingQueue<String>(numThreads * 2);
        for (int i = 0; i < numThreads; i++) {
            execSrvc.submit(new AvroTestTask(i + 1));
        }
    }

    private void showUsage() {
        System.out.println();
        System.out
                .println("usage: AvroTestclient <host> <port> <avro-batch-size> <client-batch-size> <iterations> <delay-in-millis> <num-threads> <src>");
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

        clientIp = InetAddress.getLocalHost().getHostAddress();

        String dataFormatter = new String("%s : %09d : %s");

        System.out.println();
        System.out
                .println("sample data \"" + String.format(dataFormatter, clientIp, Integer.valueOf(234), data) + "\"");
        System.out.println();

        //
        // setup client headers
        //

        for (int i = 0; i < iterations; i++) {
            showStats();
            try {
                workQueue.put(String.format(dataFormatter, clientIp, Integer.valueOf(i), data));
            }
            catch (InterruptedException e) {
                Thread.interrupted();
            }
        }

        while (numProcessed.get() < iterations) {
            showStats();
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
                Thread.interrupted();
                // ignore
            }
        }
        stopProcessing = true;
        execSrvc.shutdownNow();

        System.out.println("finished " + iterations + " iterations");
    }

    private void showStats() {
        if (2000 < System.currentTimeMillis() - start) {
            System.out.println("iteration : " + new Date().toString() + " : " + numProcessed.get() + " : "
                    + stats.getRollingStat(STAT_REQUESTS).getCountPerSecond() + " reqs/sec");
            start = System.currentTimeMillis();
        }

    }

    public static void main(String[] args) throws Exception {
        AvroTestClient client = new AvroTestClient(args);
        client.start();
    }

    @Override
    public int getRequestsPerSecond() {
        return stats.getRollingStat(STAT_REQUESTS).getCountPerSecond();
    }

    @Override
    public int getRequestAvgInMicros() {
        return stats.getRollingStat(STAT_REQUESTS).getAverageAmount();
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
    public int getAvroBatchSize() {
        return avroBatchSize;
    }

    @Override
    public void setAvroBatchSize(int avroBatchSize) {
        this.avroBatchSize = avroBatchSize;
    }

    @Override
    public int getClientBatchSize() {
        return clientBatchSize;
    }

    @Override
    public void setClientBatchSize(int clientBatchSize) {
        this.clientBatchSize = clientBatchSize;
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

    /**
     * 
     * @author bburruss
     * 
     */
    class AvroTestTask implements Runnable {
        private int workerId;
        private int sourceTypeId = 0;

        public AvroTestTask(int workerId) {
            this.workerId = workerId;
        }

        @Override
        public void run() {
            Thread.currentThread().setName("Test Worker " + workerId);
            RpcClient rpcClient = createRpcClient();

            List<Event> eventList = new ArrayList<Event>(clientBatchSize);

            try {
                while (!stopProcessing) {
                    eventList.clear();

                    if (Thread.interrupted()) {
                        continue;
                    }

                    // pull from queue until max batch hit
                    while (eventList.size() < clientBatchSize && !stopProcessing) {
                        Map<String, String> headerMap = new HashMap<String, String>();
                        headerMap.put(FlumeLogEvent.HEADER_SOURCE, String.format("%s:%03d", sourcePrefix, sourceTypeId));
                        sourceTypeId = (sourceTypeId + 1) % maxSourceTypeId ;
                        
                        headerMap.put(FlumeLogEvent.HEADER_TIMESTAMP, String.valueOf(MicrosecondsSyncClockResolution.getInstance().createTimestamp()));

                        String data;
                        try {
                            data = workQueue.take();
                            Event event = EventBuilder.withBody(data.getBytes(), headerMap);
                            eventList.add(event);
                        }
                        catch (InterruptedException e) {
                            Thread.interrupted();
                            continue;
                        }
                    }

                    if (stopProcessing) {
                        continue;
                    }

                    try {
                        long start = System.nanoTime();
                        rpcClient.appendBatch(eventList);
                        stats.updateRollingStat(STAT_REQUESTS, eventList.size(), (System.nanoTime() - start) / 1000);
                        numProcessed.addAndGet(eventList.size());
                        try {
                            Thread.sleep(delay);
                        }
                        catch (InterruptedException e) {
                            // ignore
                            Thread.interrupted();
                        }
                    }
                    catch (EventDeliveryException e) {
                        logger.error("exception while using RPC avro client", e);
                        e.printStackTrace();
                        rpcClient.close();
                        rpcClient = createRpcClient();
                    }
                }
            }
            finally {
                rpcClient.close();
            }
        }
    }

    public RpcClient createRpcClient() {
        return RpcClientFactory.getDefaultInstance(host, port, avroBatchSize);
    }
}
