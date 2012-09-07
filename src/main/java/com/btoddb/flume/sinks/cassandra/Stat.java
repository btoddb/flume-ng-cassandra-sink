package com.btoddb.flume.sinks.cassandra;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Contains the current stat of the named statistic.
 * 
 */
public class Stat
{
	private final String name;

	private int count = 0;
	private long sum = 0;
	private Long min = null;
	private Long max = null;

	private volatile long startTime;
	private volatile long endTime;

	private ReentrantReadWriteLock updateLock = new ReentrantReadWriteLock();

	public Stat(String name, long startTime)
	{
		this.name = name;
		this.startTime = startTime;
	}

	public String getName()
	{
		return name;
	}

	public void addSample(int count, long amount)
	{
		updateLock.writeLock().lock();
		try
		{
			if ( 0 < count ) {
				this.count += count;
				this.sum += amount;
				if ( null == min || amount < min ) {
					min = amount;
				}
				if ( null == max || amount > max ) {
					max = amount;
				}
				
			}
		}
		finally
		{
			updateLock.writeLock().unlock();
		}
	}

	public void addSample(Stat stat)
	{
		updateLock.writeLock().lock();
		try
		{
			if ( 0 < stat.count ) {
				this.count += stat.count;
				this.sum += stat.sum;
				if ( null == this.min || stat.min < min ) {
					min = stat.min;
				}
				if ( null == this.max || stat.max > max ) {
					max = stat.max;
				}
				
			}
		}
		finally
		{
			updateLock.writeLock().unlock();
		}
	}
	
	public void lock(long endTime)
	{
		this.endTime = endTime;
	}

	public int getCount()
	{
		return count;
	}

	public long getSum()
	{
		return sum;
	}

	public int getAverageSample()
	{
		updateLock.readLock().lock();
		try
		{
			return 0 < count ? (int) (sum / count) : 0;
		}
		finally
		{
			updateLock.readLock().unlock();
		}
	}

	public int getSamplesPerSecond()
	{
		updateLock.readLock().lock();
		try
		{
			long tmp = getEndTime() - startTime;
			if (0 < tmp)
			{
				return (int) (count / (tmp / 1000.0));
			}
			else
			{
				return 0;
			}
		}
		finally
		{
			updateLock.readLock().unlock();
		}
	}

	public long getStartTime()
	{
		return startTime;
	}

	private long getEndTime()
	{
		return 0 < endTime ? endTime : System.currentTimeMillis();
	}

	public long getMinimumSample()
	{
		return null != min ? min : 0;
	}

	public long getMaximumSample()
	{
		return null != max ? max : 0;
	}
}
