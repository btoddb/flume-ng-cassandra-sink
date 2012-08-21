package com.btoddb.flume.avroclient;

public interface AvroClientTestMXBean {

    int getRequestsPerSecond();
    int getRequestAvgInMicros();

    int getDelay();
    void setDelay(int delayInMillis);
    
    int getAvroBatchSize();
    void setAvroBatchSize(int avroBatchSize);
    
    int getClientBatchSize();
    void setClientBatchSize(int clientBatchSize);
    
    String getHost();
    
    int getPort();
    
    int getIterations();
    void setIterations(int iterations);
}
