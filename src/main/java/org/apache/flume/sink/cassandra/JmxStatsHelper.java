package org.apache.flume.sink.cassandra;

import java.util.HashMap;
import java.util.Map;

public class JmxStatsHelper {

    private Map<String, Stat> reportingMap = new HashMap<String, Stat>();
    private Map<String, Stat> calcMap = new HashMap<String, Stat>();

    private long windowStartTime;
    private long windowDuration = 15 * 1000;

    public JmxStatsHelper(long windowDuration) {
        this.windowDuration = windowDuration;
        resetStatsWindow();
    }

    public void checkStatsWindow() {
        if (System.currentTimeMillis() - windowStartTime > windowDuration) {
            resetStatsWindow();
        }
    }

    public void resetStatsWindow() {
        reportingMap = calcMap;
        long endTime = System.currentTimeMillis();
        for ( Stat st : reportingMap.values() ) {
            st.lock(endTime);
        }
        calcMap = new HashMap<String, JmxStatsHelper.Stat>();
        windowStartTime = System.currentTimeMillis();
    }

    public void update(String name, int count, long amount) {
        checkStatsWindow();
        Stat stat = getCalcStat(name);
        stat.increment(count, amount);
    }

    private Stat getCalcStat(String name) {
        Stat stat = calcMap.get(name);
        if (null == stat) {
            stat = new Stat(name, windowStartTime);
            calcMap.put(name, stat);
        }
        return stat;
    }

    public Stat getStat(String name) {
        Stat stat = reportingMap.get(name);
        if (null == stat) {
            stat = new Stat(name, windowStartTime);
            reportingMap.put(name, stat);
        }
        return stat;
    }

    /**
     * Contains the current stat of the named statistic.
     * 
     */
    class Stat {
        private final String name;

        private int count = 0;
        private long amount = 0;
        
        private long startTime;
        private long endTime;

        public Stat(String name, long startTime) {
            this.name = name;
            this.startTime = startTime;
        }

        public String getName() {
            return name;
        }

        public void increment(int count, long amount) {
            this.count += count;
            this.amount += amount;
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
            return 0 < count ? (int) (amount / count) : 0;
        }

        public int getCountPerSecond() {
            return (int) (count / ((getEndTime() - startTime) / 1000));
        }
        
        public long getStartTime() {
            return startTime;
        }
        
        private long getEndTime() {
            return 0 < endTime ? endTime : System.currentTimeMillis();
        }
    }
}
