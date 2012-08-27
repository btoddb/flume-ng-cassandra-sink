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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.cli.CliMain;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Sink.Status;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.lifecycle.LifecycleState;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class TestCassandraSink {
    private static final String CASSANDRA_HOST = "localhost";
    private static final int CASSANDRA_PORT = 9170;

    private static final DateTimeFormatter dfHourKey = new DateTimeFormatterBuilder().appendYear(4, 4)
            .appendMonthOfYear(2).appendDayOfMonth(2).appendHourOfDay(2).toFormatter();

    private Context sinkContext;
    private MemoryChannel channel;
    private CassandraSink sink;

    @Test
    public void testConfigure() {
        sinkContext.put("hosts", "host1,host2,host3");
        sinkContext.put("port", "9161");
        sinkContext.put("cluster-name", "clus1");
        sinkContext.put("keyspace-name", "ks1");
        sinkContext.put("hours-colfam", "hrs");
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
        assertEquals("hrs", repos.getHoursColFamName());
        assertEquals("recs", repos.getRecordsColFamName());
        assertEquals(1234, repos.getSocketTimeoutMillis());
        assertEquals(12, repos.getMaxConnectionsPerHost());
        assertEquals(5678, repos.getMaxExhaustedWaitMillis());

        assertEquals(channel, sink.getChannel());
        assertEquals(LifecycleState.IDLE, sink.getLifecycleState());
    }

    @Test
    public void testProcessing() throws Exception {
        Set<String> hourSet = new HashSet<String>();
        int numEvents = 120;

        // setup events
        List<Event> eventList = new ArrayList<Event>(numEvents);
        DateTime baseTime = new DateTime();
        for (int i = 0; i < numEvents; i++) {
            long tsInMillis = baseTime.plusMinutes(i).getMillis();
            String hour = sink.getRepository().getHourFromTimestamp(tsInMillis);
            hourSet.add(hour);
            String timeAsStr = String.valueOf(tsInMillis * 1000);

            Map<String, String> headerMap = new HashMap<String, String>();
            headerMap.put(FlumeLogEvent.HEADER_TIMESTAMP, timeAsStr);
            headerMap.put(FlumeLogEvent.HEADER_SOURCE, "src1");
            headerMap.put(FlumeLogEvent.HEADER_HOST, "host1");

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

        // make sure we all events are there
        int count = 0;
        for (String ts : hourSet) {
            count += countEvents(ts);
        }
        assertEquals(eventList.size(), count);

        // check cassandra for data
        checkCass(eventList);

        sink.stop();
        assertEquals(LifecycleState.STOP, sink.getLifecycleState());
    }

    @Test
    public void testReadSpeed() throws Exception {
        Set<String> hourSet = new HashSet<String>();
        int numEvents = 10000;
        int maxChannelBatchSize = 200;

        // setup events
        DateTime baseTime = new DateTime();
        Transaction transaction = null;
        for (int i = 0; i < numEvents; i++) {
            if (0 == i % maxChannelBatchSize) {
                transaction = channel.getTransaction();
                transaction.begin();
            }
            long ts = baseTime.plusMinutes(i).getMillis();
            String hour = sink.getRepository().getHourFromTimestamp(ts);
            hourSet.add(hour);
            String timeAsStr = String.valueOf(ts * 1000);

            Map<String, String> headerMap = new HashMap<String, String>();
            headerMap.put(FlumeLogEvent.HEADER_TIMESTAMP, timeAsStr);
            headerMap.put(FlumeLogEvent.HEADER_SOURCE, "src1");
            headerMap.put(FlumeLogEvent.HEADER_HOST, "host1");

            Event event = EventBuilder.withBody(("test event " + i).getBytes(), headerMap);
            channel.put(event);

            if (0 == (i + 1) % maxChannelBatchSize) {
                transaction.commit();
                transaction.close();
                transaction = null;
            }
        }

        if (null != transaction) {
            transaction.commit();
            transaction.close();
        }

        sink.start();
        assertEquals(LifecycleState.START, sink.getLifecycleState());

        while (Status.READY == sink.process()) {
            // don't stop, just get it all
        }

        // make sure we all events are there
        int readBatchSize = 1000;
        sink.getRepository().setMaxColumnBatchSize(readBatchSize);
        long start = System.nanoTime();
        int count = 0;
        for (String ts : hourSet) {
            count += countEvents(ts);
        }
        long end = System.nanoTime();

        assertEquals(numEvents, count);

        System.out.println("at " + readBatchSize + " cols/batch : "
                + (numEvents / (TimeUnit.NANOSECONDS.toSeconds(end - start))) + " events/sec");
    }

    @Test
    public void testProcessingMissingHeaders() throws Exception {
        int numEvents = 20;

        // setup events
        List<Event> eventList = new ArrayList<Event>(numEvents);
        for (int i = 0; i < numEvents; i++) {
            eventList.add(EventBuilder.withBody(("test event " + i).getBytes()));
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
        sink.getRepository().setMaxColumnBatchSize(1);
        for (int i = 0; i < eventList.size(); i++) {
            Event event = eventList.get(i);
            long tsInMicros = Long.parseLong(event.getHeaders().get(FlumeLogEvent.HEADER_TIMESTAMP));
            String cassKey = sink.getRepository().getHourFromTimestamp(tsInMicros / 1000);
            Iterator<LogEvent> eventIter = sink.getRepository().getEventsForHour(cassKey);

            boolean found = false;
            while (eventIter.hasNext()) {
                LogEvent logEvent = eventIter.next();
                if (Arrays.equals(event.getBody(), logEvent.getData())) {
                    found = true;
                    break;
                }
            }

            assertTrue("could not find event at index " + i, found);
        }
    }

    private int countEvents(String hour) {
        Iterator<LogEvent> eventIter = sink.getRepository().getEventsForHour(hour);
        int count = 0;
        while (eventIter.hasNext()) {
            eventIter.next();
            count++;
        }
        return count;
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

}
