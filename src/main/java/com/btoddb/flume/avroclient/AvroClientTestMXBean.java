package com.btoddb.flume.avroclient;

public interface AvroClientTestMXBean {

    int getRequestsPerSecond();
    int getRequestAvgInMicros();

    int getDelay();
    void setDelay(int delayInMillis);
    
    int getBatchSize();
    void setBatchSize(int batchSize);
    
    String getHost();
    
    int getPort();
    
    int getIterations();
    void setIterations(int iterations);
}
