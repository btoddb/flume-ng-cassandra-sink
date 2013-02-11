/*
 * Copyright [2013] B. Todd Burruss
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;

public class FlumeLogEvent {
//    private static final Logger logger = LoggerFactory.getLogger(FlumeLogEvent.class);

    private static final String DEFAULT_SOURCE = "not-set";
    private static final String DEFAULT_HOST = "not-set";

    public static final String HEADER_KEY = "key";
    public static final String HEADER_TIMESTAMP = "timestamp";
    public static final String HEADER_SOURCE = "src";
    public static final String HEADER_HOST = "host";

    private final Event event;

    public FlumeLogEvent(Event event) {
        this.event = event;
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
        return Long.parseLong(getHeader(HEADER_TIMESTAMP));
    }

    public String getSource() {
        return containsHeader(FlumeLogEvent.HEADER_SOURCE) ? getHeader(HEADER_SOURCE) : DEFAULT_SOURCE;
    }

    public String getHost() {
        return containsHeader(FlumeLogEvent.HEADER_HOST) ? getHeader(HEADER_HOST) : DEFAULT_HOST;
    }

    public String getKey() {
        return getHeader(HEADER_KEY);
    }

}
