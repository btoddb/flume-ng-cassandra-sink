package com.btoddb.flume.interceptors;

import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.TimestampInterceptor;

public class MicrosTimestampUniqueInterceptor implements Interceptor {
    public static String CONFIG_HEADER_NAME = "headerName";
    public static String CONFIG_PRESERVE = "preserveExisting";

    public static String HEADER_NAME_DFLT = "timestamp";
    public static boolean PRESERVE_DFLT = false;

    private MicrosecondsSyncClockResolution clock = MicrosecondsSyncClockResolution.getInstance();

    private final String headerName;
    private final boolean preserveExisting;

    /**
     * Only {@link TimestampInterceptor.Builder} can build me
     */
    private MicrosTimestampUniqueInterceptor(String headerName, boolean preserveExisting) {
        this.headerName = headerName;
        this.preserveExisting = preserveExisting;
    }

    @Override
    public void initialize() {
        // no-op
    }

    /**
     * Modifies events in-place.
     */
    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        if (preserveExisting && headers.containsKey(headerName)) {
            // we must preserve the existing timestamp
        }
        else {
            long now = clock.createTimestamp();
            headers.put(headerName, Long.toString(now));
        }
        return event;
    }

    /**
     * Delegates to {@link #intercept(Event)} in a loop.
     * 
     * @param events
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {
        // no-op
    }

    /**
     * Builder which builds new instances of the TimestampInterceptor.
     */
    public static class Builder implements Interceptor.Builder {
        private String headerName = HEADER_NAME_DFLT;
        private boolean preserveExisting = PRESERVE_DFLT;

        @Override
        public Interceptor build() {
            return new MicrosTimestampUniqueInterceptor(headerName, preserveExisting);
        }

        @Override
        public void configure(Context context) {
            headerName = context.getString(CONFIG_HEADER_NAME, HEADER_NAME_DFLT);
            preserveExisting = context.getBoolean(CONFIG_PRESERVE, PRESERVE_DFLT);
        }

    }

}
