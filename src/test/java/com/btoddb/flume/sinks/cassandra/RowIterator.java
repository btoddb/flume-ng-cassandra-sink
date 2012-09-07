package com.btoddb.flume.sinks.cassandra;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

public class RowIterator<N> {
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    
    private Serializer<N> colNameSerializer;
    private int maxRowCount = 1000;
    private int maxColumnCount = 1000;

    public RowIterator(Serializer<N> colNameSerializer) {
        this.colNameSerializer = colNameSerializer;
    }

    /**
     * Iterates until RowOperator returns false, or out of keys.
     * 
     * @param cluster
     * @param keyspace
     * @param colFamName
     * @param lastRowKey
     * @param op
     * 
     * @return true if terminated by RowOperator, false otherwise
     */
    public boolean execute(Cluster cluster, Keyspace keyspace, String colFamName, byte[] lastRowKey, RowOperator<N> op) {
        RangeSlicesQuery<byte[], N, byte[]> q =
                HFactory.createRangeSlicesQuery(keyspace, BytesArraySerializer.get(), colNameSerializer, BytesArraySerializer.get());
        q.setColumnFamily(colFamName);
        q.setRange(null, null, false, maxColumnCount);
        q.setRowCount(maxRowCount);
        byte[] startKey = EMPTY_BYTE_ARRAY;

        boolean continueSearching = true;
        while (continueSearching) {
            boolean skipFirstRow = 0 < startKey.length;
            q.setKeys(startKey, EMPTY_BYTE_ARRAY);
            QueryResult<OrderedRows<byte[], N, byte[]>> result = q.execute();
            continueSearching =
                    null != result
                            && (!skipFirstRow && 0 < result.get().getCount() || skipFirstRow
                                    && 1 < result.get().getCount());
            if (!continueSearching) {
                break;
            }

            for (Row<byte[], N, byte[]> row : result.get().getList()) {
                startKey = row.getKey();
                if (skipFirstRow) {
                    skipFirstRow = false;
                    continue;
                }

                ColumnSlice<N, byte[]> colSlice = row.getColumnSlice();
                if (null != colSlice && !colSlice.getColumns().isEmpty()) {
                    if (!op.execute(row.getKey(), colSlice)) {
                        return true;
                    }
                }

            }
        }
        
        return false;
    }

    public Serializer<N> getColNameSerializer() {
        return colNameSerializer;
    }

    public void setColNameSerializer(Serializer<N> nameSerializer) {
        this.colNameSerializer = nameSerializer;
    }

    public int getMaxRowCount() {
        return maxRowCount;
    }

    public void setMaxRowCount(int maxRowCount) {
        this.maxRowCount = maxRowCount;
    }

    public interface RowOperator<N> {
        boolean execute(byte[] key, ColumnSlice<N, byte[]> colSlice);
    }

}
