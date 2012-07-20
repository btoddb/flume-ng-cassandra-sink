package com.btoddb.flume.sinks.cassandra;

public interface CassandraSinkMBean {

	int getMaxSaveBatchSize();
	void setMaxSaveBatchSize(int maxBatchSize);
	
//	int getLastSaveBatchSize();
//	long getLastSaveDurationInMicros();
	
	int getSaveAvgInMicros();
	int getSavesPerSecond();
	
    int getTakeAvgInMicros();
    int getTakesPerSecond();	
	
    int getBatchSizeAvg();
}
