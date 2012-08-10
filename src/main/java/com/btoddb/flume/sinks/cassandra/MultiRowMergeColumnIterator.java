package com.btoddb.flume.sinks.cassandra;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.commons.lang.NotImplementedException;

public class MultiRowMergeColumnIterator implements Iterator<LogEvent> {
    private RowManager[] rowArr;
    private final Comparator<UUID> columnComparator;
    private final ColumnTranslator<UUID, byte[], LogEvent> columnTranslator;
    private boolean hasMore = false;
    private int maxColumnBatchSize;

    public MultiRowMergeColumnIterator(Keyspace keyspace, String colFamName, ByteBuffer[] keyList,
            Comparator<UUID> columnComparator, ColumnTranslator<UUID, byte[], LogEvent> columnTranslator,
            int maxColumnBatchSize) {
        this.columnComparator = columnComparator;
        this.columnTranslator = columnTranslator;
        this.maxColumnBatchSize = maxColumnBatchSize;

        if (null == keyList) {
            rowArr = new RowManager[0];
            return;
        }

        rowArr = new RowManager[keyList.length];
        for (int i=0;i < keyList.length;i++ ) {
            ByteBuffer key = keyList[i];
            rowArr[i] = new RowManager(keyspace, colFamName, key.array(), maxColumnBatchSize);

            // i want the manager to load data, so keep "or" clause in this order
            hasMore = rowArr[i].hasMoreData() || hasMore;
        }
    }

    public LogEvent getNext() {
        RowManager winner = null;
        hasMore = false;
        for (int i = 0; i < rowArr.length; i++) {
            if (null == rowArr[i] || !rowArr[i].hasMoreData()) {
                continue;
            }
            
            if (null == winner) {
                winner = rowArr[i];
            }
            else if (0 > columnComparator.compare(rowArr[i].getNextColumnName(), winner.getNextColumnName())) {
                winner = rowArr[i];
            }
            else {
                // if we get here, we know there is more data, but the data wasn't the "newest"
                hasMore = true;
            }
        }

        if (null == winner) {
            return null;
        }

        List<LogEvent> eventList = columnTranslator.translate(Collections.singletonList(winner.getNextColumn()));
        winner.incrementIndex();

        // only check for more, if we don't know from previous traversal
        hasMore = hasMore || checkForMore();
        
        return eventList.iterator().next();
    }

    private boolean checkForMore() {
        for ( int i=rowArr.length;i > 0;i-- ) {
            if ( rowArr[i-1].hasMoreData() ) {
                return true;
            }
        }
        return false;
    }
    
    @Override
    public boolean hasNext() {
        return hasMore;
    }

    @Override
    public LogEvent next() {
        LogEvent event = getNext();
        if ( null != event ) {
            return event;
        }
        throw new IllegalStateException("Reached end of iterator - cannot return any data");
    }

    @Override
    public void remove() {
        throw new NotImplementedException();
    }

    public int getMaxColumnBatchSize() {
        return maxColumnBatchSize;
    }

    public void setMaxColumnBatchSize(int maxColumnBatchSize) {
        this.maxColumnBatchSize = maxColumnBatchSize;
        for (int i=0;i < rowArr.length;i++ ) {
            rowArr[i].setMaxBatchSize(this.maxColumnBatchSize);
        }
    }
}


class RowManager {
    private final Keyspace keyspace;
    private final String colFamName;
    private final byte[] key;
    
    private int maxBatchSize;
    private List<HColumn<UUID, byte[]>> colList = Collections.emptyList();
    private int nextColIndex = 0;
    private boolean possiblyHasMore = true;
    private UUID startColName = null;

    public RowManager(Keyspace keyspace, String colFamName, byte[] key, int maxBatchSize) {
        this.keyspace = keyspace;
        this.colFamName = colFamName;
        this.key = key;
        this.maxBatchSize = maxBatchSize;
    }

    public boolean hasMoreData() {
        if (nextColIndex < colList.size()) {
            return true;
        }
        else if (!possiblyHasMore) {
            return false;
        }

        loadMore();

        return possiblyHasMore;
    }

    public HColumn<UUID, byte[]> getNextColumn() {
        return hasMoreData() ? colList.get(nextColIndex) : null;
    }

    public UUID getNextColumnName() {
        HColumn<UUID, byte[]> col = getNextColumn();
        return null != col ? col.getName() : null;
    }

    public void incrementIndex() {
        nextColIndex++;
    }

    // call this method when you need more data.  it will also mark the row as exhausted 
    private void loadMore() {
        SliceQuery<byte[], UUID, byte[]> dayQuery = HFactory.createSliceQuery(keyspace, BytesArraySerializer.get(),
                UUIDSerializer.get(), BytesArraySerializer.get());
        dayQuery.setColumnFamily(colFamName);
        dayQuery.setKey(key);
        dayQuery.setRange(startColName, null, false, null != startColName ? maxBatchSize+1 : maxBatchSize);

        QueryResult<ColumnSlice<UUID, byte[]>> qRes = dayQuery.execute();
        ColumnSlice<UUID, byte[]> slice = null != qRes ? qRes.get() : null;
        if (null == slice) {
            possiblyHasMore = false;
            colList = Collections.emptyList();
            nextColIndex = 0;
            return;
        }

        colList = slice.getColumns();
        if (null == colList || colList.isEmpty()) {
            possiblyHasMore = false;
            colList = Collections.emptyList();
            nextColIndex = 0;
            return;
        }

        if (null != startColName && 1 == colList.size()) {
            possiblyHasMore = false;
            colList = Collections.emptyList();
            nextColIndex = 0;
            return;
        }

//        colList = slice.getColumns();

        // skip past first column read if this is *not* the first page of columns
        if (null != startColName) {
            nextColIndex = 1;
        }

        // set start column name to last one in this page
        startColName = colList.get(colList.size() - 1).getName();
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

}
