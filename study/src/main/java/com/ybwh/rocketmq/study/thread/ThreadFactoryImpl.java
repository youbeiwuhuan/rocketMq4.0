package com.ybwh.rocketmq.study.thread;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 使用线程池一定要用自己的线程工程
 *
 */
public class ThreadFactoryImpl implements ThreadFactory {
	/**
	 * 线程的索引号
	 */
	private final AtomicLong threadIndex = new AtomicLong(0);
	/**
	 * 线程名的前缀
	 */
	private final String threadNamePrefix;

	/**
	 * @param threadNamePrefix
	 *            线程名的前缀
	 */
	public ThreadFactoryImpl(final String threadNamePrefix) {
		this.threadNamePrefix = threadNamePrefix;
	}

	@Override
	public Thread newThread(Runnable r) {
		return new Thread(r, threadNamePrefix + this.threadIndex.incrementAndGet());

	}
}