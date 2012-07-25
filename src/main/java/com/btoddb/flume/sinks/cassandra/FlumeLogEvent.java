package com.btoddb.flume.sinks.cassandra;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.btoddb.flume.interceptors.MicrosecondsSyncClockResolution;

public class FlumeLogEvent {
    private static final Logger logger = LoggerFactory.getLogger(FlumeLogEvent.class);

    private static final String DEFAULT_SOURCE = "not-set";
    private static final String DEFAULT_HOST = "not-set";

    public static final String HEADER_TIMESTAMP = "timestamp";
    public static final String HEADER_SOURCE = "src";
    public static final String HEADER_HOST = "host";

    private final Event event;

    // assume time units coming in from event is of this type. Can be set in constructor.
    private final TimeUnit timeUnit;

    public FlumeLogEvent(Event event, TimeUnit timeUnit) {
        this.event = event;
        this.timeUnit = timeUnit;
    }

    public Event getEvent() {
        return event;
    }

    public void addHeader(String name, String value) {
        getHeaderMap().put(name, value);
    }

    public Map<String, String> getHeaderMap() {
        Map<String, String> headerMap = event.getHeaders();
        if (null == headerMap) {
            headerMap = new HashMap<String, String>();
            event.setHeaders(headerMap);
        }
        return headerMap;
    }

    public String getHeader(String name) {
        return getHeaderMap().get(name);
    }

    public boolean containsHeader(String name) {
        return getHeaderMap().containsKey(name);
    }

    public long getTimestamp() {
        long ts;
        // headers must be stored
        if (containsHeader(HEADER_TIMESTAMP)) {
            ts = timeUnit.toMicros(Long.parseLong(getHeader(HEADER_TIMESTAMP)));
        }
        else {
            ts = timeUnit.toMicros(MicrosecondsSyncClockResolution.getInstance().createTimestamp());
            event.getHeaders().put(HEADER_TIMESTAMP, String.valueOf(ts));
            logger.info("Event is missing '" + FlumeLogEvent.HEADER_TIMESTAMP + "' header - using current time");
        }
        return ts;
    }

    public String getSource() {
        return containsHeader(FlumeLogEvent.HEADER_SOURCE) ? getHeader(HEADER_SOURCE) : DEFAULT_SOURCE;
    }

    public String getHost() {
        return containsHeader(FlumeLogEvent.HEADER_HOST) ? getHeader(HEADER_HOST) : DEFAULT_HOST;
    }

}
