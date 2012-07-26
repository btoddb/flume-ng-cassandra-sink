package com.btoddb.flume.sinks.cassandra;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class JmxStatsHelper {

    private Map<String, Stat> reportingMap = new HashMap<String, Stat>();
    private Map<String, Stat> calcMap = new HashMap<String, Stat>();

    private long windowStartTime;
    private long windowDuration = 15 * 1000;

    private volatile boolean checkingWindow = false;
    private Object checkingWindowMonitor = new Object();
    private Object updateMonitor = new Object();
    private Object createStatMonitor = new Object();

    public JmxStatsHelper(long windowDuration) {
        this.windowDuration = windowDuration;
        resetStatsWindow();
    }

    public void checkStatsWindow() {
        if (checkingWindow) {
            return;
        }

        boolean gotAccess = false;
        synchronized (checkingWindowMonitor) {
            if (!checkingWindow) {
                checkingWindow = true;
                gotAccess = true;
            }
        }

        if (gotAccess) {
            try {
                if (System.currentTimeMillis() - windowStartTime > windowDuration) {
                    resetStatsWindow();
                }
            }
            finally {
                checkingWindow = false;
            }
        }
    }

    public void resetStatsWindow() {
        synchronized (updateMonitor) {
            reportingMap = calcMap;
            calcMap = new HashMap<String, JmxStatsHelper.Stat>();
            windowStartTime = System.currentTimeMillis();
        }

        // lock down window end time on reporting map
        long endTime = System.currentTimeMillis();
        for (Stat st : reportingMap.values()) {
            st.lock(endTime);
        }
    }

    public void update(String name, int count, long amount) {
        checkStatsWindow();
        Stat stat = getCalcStat(name);
        stat.increment(count, amount);
    }

    private Stat getCalcStat(String name) {
        Stat stat = calcMap.get(name);
        if (null == stat) {
            stat = createNewStat(calcMap, name, windowStartTime);
        }
        return stat;
    }

    public Stat getStat(String name) {
        Stat stat = reportingMap.get(name);
        if (null == stat) {
            stat = createNewStat(reportingMap, name, windowStartTime);
        }
        return stat;
    }

    private Stat createNewStat(Map<String, Stat> map, String name, long windowStartTime) {
        Stat stat = map.get(name);
        if ( null != stat ) {
            return stat;
        }
        
        synchronized (createStatMonitor) {
            stat = map.get(name);
            if (null == stat) {
                stat = new Stat(name, windowStartTime);
                map.put(name, stat);
            }
            return stat;
        }
    }

    /**
     * Contains the current stat of the named statistic.
     * 
     */
    public class Stat {
        private final String name;

        private int count = 0;
        private long amount = 0;

        private volatile long startTime;
        private volatile long endTime;

        private ReentrantReadWriteLock updateLock = new ReentrantReadWriteLock();

        public Stat(String name, long startTime) {
            this.name = name;
            this.startTime = startTime;
        }

        public String getName() {
            return name;
        }

        public void increment(int deltaCount, long deltaAmount) {
            updateLock.writeLock().lock();
            try {
                this.count += deltaCount;
                this.amount += deltaAmount;
            }
            finally {
                updateLock.writeLock().unlock();
            }
        }

        public void lock(long endTime) {
            this.endTime = endTime;
        }

        public int getCount() {
            return count;
        }

        public long getAmount() {
            return amount;
        }

        public int getAverageAmount() {
            updateLock.readLock().lock();
            try {
                return 0 < count ? (int) (amount / count) : 0;
            }
            finally {
                updateLock.readLock().unlock();
            }
        }

        public int getCountPerSecond() {
            updateLock.readLock().lock();
            try {
                long tmp = getEndTime() - startTime;
                if (0 < tmp) {
                    return (int) (count / (tmp / 1000.0));
                }
                else {
                    return 0;
                }
            }
            finally {
                updateLock.readLock().unlock();
            }
        }

        public long getStartTime() {
            return startTime;
        }

        private long getEndTime() {
            return 0 < endTime ? endTime : System.currentTimeMillis();
        }
    }
}
