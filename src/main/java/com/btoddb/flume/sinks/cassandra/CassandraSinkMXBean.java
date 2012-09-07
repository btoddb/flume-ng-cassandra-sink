package com.btoddb.flume.sinks.cassandra;

public interface CassandraSinkMXBean {

	int getMaxSaveBatchSize();
	void setMaxSaveBatchSize(int maxBatchSize);
	
	int getSaveAvgInMicros();
	int getSavesPerSecond();
	
    int getTakeAvgInMicros();
    int getTakesPerSecond();	
	
    int getBatchSizeAvg();
    
}
