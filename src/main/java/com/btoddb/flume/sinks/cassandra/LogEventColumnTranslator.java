package com.btoddb.flume.sinks.cassandra;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;

public class LogEventColumnTranslator implements ColumnTranslator<UUID, byte[], LogEvent> {
    private Keyspace keyspace;
    private String recordsColFamName;
    
    public LogEventColumnTranslator(Keyspace keyspace, String recordsColFamName) {
        this.keyspace = keyspace;
        this.recordsColFamName = recordsColFamName;
    }

    @Override
    public List<LogEvent> translate(List<HColumn<UUID, byte[]>> colList) {
        if ( null == colList || colList.isEmpty() ) {
            return Collections.emptyList();
        }
        
        List<UUID> recordKeyList = new ArrayList<UUID>(colList.size());
        for ( HColumn<UUID, byte[]> col : colList) {
            recordKeyList.add(col.getName());
        }
        
        return getRecords(recordKeyList);
    }

    private List<LogEvent> getRecords(List<UUID> recordKeyList) {
        MultigetSliceQuery<UUID, String, byte[]> q = HFactory.createMultigetSliceQuery(keyspace, UUIDSerializer.get(),
                StringSerializer.get(), BytesArraySerializer.get());
        q.setColumnFamily(recordsColFamName);
        q.setKeys(recordKeyList);
        q.setRange(null, null, false, 100);

        // get the records
        // TODO:BTB - this will need to be paginated
        QueryResult<Rows<UUID, String, byte[]>> result = q.execute();

        // make sure we got something and dig for the data
        Rows<UUID, String, byte[]> rows = null != result ? result.get() : null;
        Iterator<Row<UUID, String, byte[]>> iter = null != rows ? rows.iterator() : null;
        if (null == iter) {
            return Collections.emptyList();
        }

        // iterate over the rows returned
        List<LogEvent> logEventList = new LinkedList<LogEvent>();
        while (iter.hasNext()) {
            Row<UUID, String, byte[]> row = iter.next();
            ColumnSlice<String, byte[]> slice = null != row ? row.getColumnSlice() : null;
            List<HColumn<String, byte[]>> colList = null != slice ? slice.getColumns() : null;

            // it is possible to get a row with no columns (has tombstones)
            if (null != colList && !colList.isEmpty()) {
                HColumn<String, byte[]> tmp = slice.getColumnByName("src");
                String src = new String(tmp.getValue());
                String host = new String(slice.getColumnByName("host").getValue());
                byte[] data = slice.getColumnByName("data").getValue();

                LogEvent logEvt = new LogEvent(TimeUUIDUtils.getTimeFromUUID(row.getKey()), src, host, data);
                logEventList.add(logEvt);
            }
        }

        return logEventList;
    }

}
