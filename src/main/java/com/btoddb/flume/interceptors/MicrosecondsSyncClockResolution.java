package com.btoddb.flume.interceptors;

/**
 * Synchronized Milliseconds Resolution used to create timestamp.
 * 
 * <p/>
 * Copied from hector-client.org and modified to not include dependencies.
 */
public class MicrosecondsSyncClockResolution {
    private static final long ONE_THOUSAND = 1000L;

    private static final MicrosecondsSyncClockResolution INSTANCE = new MicrosecondsSyncClockResolution();

    /**
     * The last time value issued. Used to try to prevent duplicates.
     */
    private static long lastTime = -1;

    private MicrosecondsSyncClockResolution() {}
    
    public static MicrosecondsSyncClockResolution getInstance() {
        return INSTANCE;
    }

    public long createTimestamp() {
        // The following simulates a microseconds resolution by advancing a static counter
        // every time a client calls the createClock method, simulating a tick.
        long us = System.nanoTime() / ONE_THOUSAND;
        // Synchronized to guarantee unique time within and across threads.
        synchronized (MicrosecondsSyncClockResolution.class) {
            if (us > lastTime) {
                lastTime = us;
            }
            else {
                // the time i got from the system is equals or less
                // (hope not - clock going backwards)
                // One more "microsecond"
                us = ++lastTime;
            }
        }
        return us;
    }

}
