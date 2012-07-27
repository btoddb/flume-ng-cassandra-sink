package com.btoddb.flume.channels;

public interface BerkeleyChannelMXBean {

    int getTakesPerSecond();
    int getTakeAvgInMicros();

    int getPutsPerSecond();
    int getPutAvgInMicros();
    
    long getChannelSize();
    
    long getMaxChannelSize();
    void setMaxChannelSize(long maxChannelSize);
    
    long getNumberRollbacks();
    long getNumberCommits();
    
    long getMaxPutWaitTime();
    void setMaxPutWaitTime(long maxPutWaitTime);
}
