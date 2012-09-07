
package com.btoddb.flume.sinks.cassandra;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class JmxStatsHelper
{

	private Map<String, Stat> reportingMap = new HashMap<String, Stat>();
	private Map<String, Stat> calcMap = new HashMap<String, Stat>();
	private Map<String, AtomicLong> counterMap = new HashMap<String, AtomicLong>();

	private long windowStartTime;
	private long calcWindowSizeInMillis = 15 * 1000;

	private volatile boolean checkingWindow = false;
	private Object checkingWindowMonitor = new Object();
	private Object updateRollingMonitor = new Object();
	private Object createStatMonitor = new Object();

	public JmxStatsHelper(long calcWindowSizeInMillis)
	{
		this.calcWindowSizeInMillis = calcWindowSizeInMillis;
		resetRollingStatsWindow();
	}

	public void checkStatsWindow()
	{
		if (checkingWindow)
		{
			return;
		}

		boolean gotAccess = false;
		synchronized (checkingWindowMonitor)
		{
			if (!checkingWindow)
			{
				checkingWindow = true;
				gotAccess = true;
			}
		}

		if (gotAccess)
		{
			try
			{
				if (System.currentTimeMillis() - windowStartTime > calcWindowSizeInMillis)
				{
					resetRollingStatsWindow();
				}
			}
			finally
			{
				checkingWindow = false;
			}
		}
	}

	public void resetRollingStatsWindow()
	{
		synchronized (updateRollingMonitor)
		{
			reportingMap = calcMap;
			calcMap = new HashMap<String, Stat>();
			windowStartTime = System.currentTimeMillis();
		}

		// lock down window end time on reporting map
		long endTime = System.currentTimeMillis();
		for (Stat st : reportingMap.values())
		{
			st.lock(endTime);
		}
	}

	/**
	 * Intended to get the sample name into the system, but not have any data associated with it. This is a good
	 * thing if you are querying this object to populate JMX bean information.
	 * 
	 * @param name
	 */
	public void addRollingSampleName(String name)
	{
		addRollingSample(name, 0, 0);
	}

	public void addRollingSample(String name, int count, long amount)
	{
		checkStatsWindow();
		Stat stat = getCalcStat(name);
		stat.addSample(count, amount);
	}

	private Stat getCalcStat(String name)
	{
		Stat stat = calcMap.get(name);
		if (null == stat)
		{
			stat = createNewRollingStat(calcMap, name, windowStartTime);
		}
		return stat;
	}

	public Stat getRollingStat(String name)
	{
		Stat stat = reportingMap.get(name);
		if (null == stat)
		{
			stat = createNewRollingStat(reportingMap, name, windowStartTime);
		}
		return stat;
	}

	public Set<String> getRollingStatNames()
	{
		return reportingMap.keySet();
	}

	public Set<String> getCounterStatNames()
	{
		return counterMap.keySet();
	}

	private Stat createNewRollingStat(Map<String, Stat> map, String name, long windowStartTime)
	{
		Stat stat = map.get(name);
		if (null != stat)
		{
			return stat;
		}

		synchronized (createStatMonitor)
		{
			stat = map.get(name);
			if (null == stat)
			{
				stat = new Stat(name, windowStartTime);
				map.put(name, stat);
			}
			return stat;
		}
	}

	private AtomicLong createNewCounterStat(Map<String, AtomicLong> map, String name)
	{
		AtomicLong stat = map.get(name);
		if (null != stat)
		{
			return stat;
		}

		synchronized (createStatMonitor)
		{
			stat = map.get(name);
			if (null == stat)
			{
				stat = new AtomicLong();
				map.put(name, stat);
			}
			return stat;
		}
	}

	public void incrementCounter(String name, long count)
	{
		getCounterStatInternal(name).addAndGet(count);
	}

	private AtomicLong getCounterStatInternal(String name)
	{
		AtomicLong stat = counterMap.get(name);
		if (null == stat)
		{
			stat = createNewCounterStat(counterMap, name);
		}
		return stat;
	}

	public long getCounterValue(String name)
	{
		return getCounterStatInternal(name).get();
	}
}
