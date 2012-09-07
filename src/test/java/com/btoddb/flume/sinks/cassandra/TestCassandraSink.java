/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.btoddb.flume.sinks.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;

import org.apache.cassandra.cli.CliMain;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.lifecycle.LifecycleState;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCassandraSink {
    private static final String CASSANDRA_HOST = "localhost";
    private static final int CASSANDRA_PORT = 9170;

    // private static final DateTimeFormatter dfHourKey = new DateTimeFormatterBuilder().appendYear(4, 4)
    // .appendMonthOfYear(2).appendDayOfMonth(2).appendHourOfDay(2).toFormatter();

    private Context sinkContext;
    private MemoryChannel channel;
    private CassandraSink sink;

    @Test
    public void testConfigure() {
        sinkContext.put("hosts", "host1,host2,host3");
        sinkContext.put("port", "9161");
        sinkContext.put("cluster-name", "clus1");
        sinkContext.put("keyspace-name", "ks1");
        sinkContext.put("records-colfam", "recs");
        sinkContext.put("socket-timeout-millis", "1234");
        sinkContext.put("max-conns-per-host", "12");
        sinkContext.put("max-exhausted-wait-millis", "5678");

        Configurables.configure(sink, sinkContext);

        CassandraSinkRepository repos = sink.getRepository();

        assertEquals("host1,host2,host3", repos.getHosts());
        assertEquals(9161, repos.getPort());
        assertEquals("clus1", repos.getClusterName());
        assertEquals("ks1", repos.getKeyspaceName());
        assertEquals("recs", repos.getRecordsColFamName());
        assertEquals(1234, repos.getSocketTimeoutMillis());
        assertEquals(12, repos.getMaxConnectionsPerHost());
        assertEquals(5678, repos.getMaxExhaustedWaitMillis());

        assertEquals(channel, sink.getChannel());
        assertEquals(LifecycleState.IDLE, sink.getLifecycleState());
    }

    @Test
    public void testProcessing() throws Exception {
        // Set<String> hourSet = new HashSet<String>();
        int numEvents = 120;

        // setup events
        List<Event> eventList = new ArrayList<Event>(numEvents);
        DateTime baseTime = new DateTime();
        for (int i = 0; i < numEvents; i++) {
            long tsInMillis = baseTime.plusMinutes(i).getMillis();
            // String hour = sink.getRepository().getHourFromTimestamp(tsInMillis);
            // hourSet.add(hour);
            String timeAsStr = String.valueOf(tsInMillis * 1000);

            Map<String, String> headerMap = new HashMap<String, String>();
            headerMap.put(FlumeLogEvent.HEADER_TIMESTAMP, timeAsStr);
            headerMap.put(FlumeLogEvent.HEADER_SOURCE, "src1");
            headerMap.put(FlumeLogEvent.HEADER_HOST, "host1");
            headerMap.put(FlumeLogEvent.HEADER_KEY, String.valueOf(i));

            eventList.add(EventBuilder.withBody(("test event " + i).getBytes(), headerMap));
        }

        sink.start();
        assertEquals(LifecycleState.START, sink.getLifecycleState());

        // put event in channel
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        for (Event event : eventList) {
            channel.put(event);
        }
        transaction.commit();
        transaction.close();

        sink.process();

        assertEquals(eventList.size(), countAllEventsInRepository());

        // check cassandra for data
        checkCass(eventList);

        sink.stop();
        assertEquals(LifecycleState.STOP, sink.getLifecycleState());
    }

    @Test
    public void testProcessingMissingHeaders() throws Exception {
        int numEvents = 20;

        // setup events
        List<Event> eventList = new ArrayList<Event>(numEvents);
        for (int i = 0; i < numEvents; i++) {
            Map<String, String> headerMap = new HashMap<String, String>();
            headerMap.put(FlumeLogEvent.HEADER_TIMESTAMP, String.valueOf(System.currentTimeMillis()));
//            headerMap.put(FlumeLogEvent.HEADER_SOURCE, "src1");
//            headerMap.put(FlumeLogEvent.HEADER_HOST, "host1");
            headerMap.put(FlumeLogEvent.HEADER_KEY, String.valueOf(i));

            eventList.add(EventBuilder.withBody(("test event " + i).getBytes(), headerMap));
        }

        sink.start();
        assertEquals(LifecycleState.START, sink.getLifecycleState());

        // put event in channel
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        for (Event event : eventList) {
            channel.put(event);
        }
        transaction.commit();
        transaction.close();

        sink.process();

        // assertEquals(eventList.size(), countEvents());

        // check cassandra for data
        checkCass(eventList);

        sink.stop();
        assertEquals(LifecycleState.STOP, sink.getLifecycleState());
    }

    // ----------------------

    private void checkCass(List<Event> eventList) {
        CassandraSinkRepository repos = sink.getRepository();
        for (int i = 0; i < eventList.size(); i++) {
            final Event event = eventList.get(i);
            RowIterator<String> rowIter = new RowIterator<String>(StringSerializer.get());
            boolean found =
            rowIter.execute(repos.getCluster(), repos.getKeyspace(), repos.getRecordsColFamName(), null,
                    new RowIterator.RowOperator<String>() {
                        @Override
                        public boolean execute(byte[] key, ColumnSlice<String, byte[]> colSlice) {
                            HColumn<String, byte[]> body = colSlice.getColumnByName("data");
                            return !Arrays.equals(event.getBody(), body.getValue());
                        }
                    });

            
            assertTrue("could not find event at index " + i, found);
        }
    }

    @Before
    public void setupTest() throws Exception {
        Context channelContext = new Context();
        channelContext.put("capacity", "10000");
        channelContext.put("transactionCapacity", "200");

        channel = new MemoryChannel();
        channel.setName("junitChannel");
        Configurables.configure(channel, channelContext);

        sinkContext = new Context();
        sinkContext.put("hosts", CASSANDRA_HOST);
        sinkContext.put("port", String.valueOf(CASSANDRA_PORT));
        sinkContext.put("cluster-name", "Logging");
        sinkContext.put("keyspace-name", "logs");
        sinkContext.put("max-conns-per-host", "1");
        sinkContext.put("scatter-value", "5");
        sinkContext.put("batch-size", "200");

        sink = new CassandraSink();
        Configurables.configure(sink, sinkContext);
        sink.setChannel(channel);

        // configure cassandra keyspace
        createKeyspaceFromCliFile(this.getClass().getResource("/cassandra-schema.txt").getFile());
    }

    protected void createKeyspaceFromCliFile(String fileName) throws Exception {
        // new error/output streams for CliSessionState
        ByteArrayOutputStream errStream = new ByteArrayOutputStream();
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();

        // checking if we can connect to the running cassandra node on localhost
        CliMain.connect(CASSANDRA_HOST, CASSANDRA_PORT);

        try {
            // setting new output stream
            CliMain.sessionState.setOut(new PrintStream(outStream));
            CliMain.sessionState.setErr(new PrintStream(errStream));

            // read schema from file
            BufferedReader fr = new BufferedReader(new FileReader(fileName));
            String line = null;
            StringBuffer sb = new StringBuffer();
            while (null != (line = fr.readLine())) {
                line = line.trim();
                if (line.startsWith("#")) {
                    continue;
                }
                else if (line.startsWith("quit")) {
                    break;
                }

                sb.append(line);
                sb.append(' ');
                if (line.endsWith(";")) {
                    try {
                        CliMain.processStatement(sb.toString());
                        // String result = outStream.toString();
                        outStream.toString();

                        outStream.reset(); // reset stream so we have only
                                           // output
                                           // from
                                           // next statement all the time
                        errStream.reset(); // no errors to the end user.
                    }
                    catch (Throwable e) {
                        if (sb.toString().startsWith("drop keyspace") && !e.getMessage().endsWith("not found.")) {
                            e.printStackTrace();
                        }
                    }

                    sb = new StringBuffer();
                }
            }
        }
        finally {
            if (CliMain.isConnected()) {
                CliMain.disconnect();
            }
        }
    }

    @BeforeClass
    public static void setupCassandra() throws Exception {
        EmbeddedServerHelper helper = new EmbeddedServerHelper("/cassandra-junit.yaml");
        helper.setup();

        // make sure startup finished and can cannect
        for (int i = 0; i < 10; i++) {
            try {
                CliMain.connect("localhost", 9170);
                CliMain.disconnect();
                break;
            }
            catch (Throwable e) {
                // wait, then retry
                Thread.sleep(100);
            }
        }
    }

    @AfterClass
    public static void teardown() throws IOException {
        EmbeddedServerHelper.teardown();

    }

    private int countAllEventsInRepository() {
        CassandraSinkRepository repos = sink.getRepository();
        final AtomicInteger count = new AtomicInteger();
        
        RowIterator<String> rowIter = new RowIterator<String>(StringSerializer.get());
        rowIter.execute(repos.getCluster(), repos.getKeyspace(), repos.getRecordsColFamName(), null,
                new RowIterator.RowOperator<String>() {
                    @Override
                    public boolean execute(byte[] key, ColumnSlice<String, byte[]> colSlice) {
                        count.incrementAndGet();
                        return true;
                    }
                });

        return count.get();
    }

}
