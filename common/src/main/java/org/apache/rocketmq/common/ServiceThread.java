/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RocketMq所有服务任务的父类，实际上是Thread包装类
 * 实现了等待、唤醒、停止的逻辑
 *
 */
public abstract class ServiceThread implements Runnable {
    private static final Logger STLOG = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);
    private static final long JOIN_TIME = 90 * 1000;

    /**
     * 工作线程
     */
    protected final Thread thread;
    /**
     * 用来实现线程的等待和唤醒唤醒逻辑
     */
    protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);
    /**
     * 标识线程是被唤醒还是仍在等待
     */
    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);
    /**
     * 线程是否停止
     */
    protected volatile boolean stopped = false;

    public ServiceThread() {
        this.thread = new Thread(this, this.getServiceName());
    }

    public abstract String getServiceName();

    public void start() {
        this.thread.start();
    }

    public void shutdown() {
        this.shutdown(false);
    }

    /**
     * 停止服务执行,释放资源
     * 
     * @param interrupt 是否被中断工作线程
     */
    public void shutdown(final boolean interrupt) {
        this.stopped = true;
        STLOG.info("shutdown thread " + this.getServiceName() + " interrupt " + interrupt);

        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        try {
            if (interrupt) {
                this.thread.interrupt();//中断waitPoint.await()
            }

            long beginTime = System.currentTimeMillis();
            if (!this.thread.isDaemon()) {//如果工作线程是守护线程，则等待工作线程执行完
                this.thread.join(this.getJointime());
            }
            long eclipseTime = System.currentTimeMillis() - beginTime;
            STLOG.info("join thread " + this.getServiceName() + " eclipse time(ms) " + eclipseTime + " "
                + this.getJointime());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public long getJointime() {
        return JOIN_TIME;
    }

    /**
     * 打上停止的标记
     */
    public void stop() {
        this.stop(false);
    }

    /**
     * 停止服务运行。interrupt为false只是打上停止的标记，
     * ServiceThread所有的子类都在run方法里面调用isStopped判断实现停止逻辑.
     * 
     * @param interrupt  是否强行中断线程,强行中断可能导致run方法中流程执行一半终止
     */
    public void stop(final boolean interrupt) {
        this.stopped = true;
        STLOG.info("stop thread " + this.getServiceName() + " interrupt " + interrupt);

        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        if (interrupt) {
            this.thread.interrupt();
        }
    }

    /**
     * 打上停止的标记，ServiceThread所有的子类都在run方法里面调用isStopped判断实现停止逻辑.
     */
    public void makeStop() {
        this.stopped = true;
        STLOG.info("makestop thread " + this.getServiceName());
    }

    /**
     * 唤醒等待，让该线程继续运行
     */
    public void wakeup() {
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }
    }

    /**
     * 等待一定时间后再运行,时间未到可以被wakeup唤醒
     * 
     * 
     * @param interval
     */
    protected void waitForRunning(long interval) {
    	
    	
        if (hasNotified.compareAndSet(true, false)) {//设置为等待状态
        	//等待逻辑还没开始就被wakeup唤醒的情况
            this.onWaitEnd();
            return;
        }

        //以下是等待逻辑
        //entry to wait
        waitPoint.reset();

        try {
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            hasNotified.set(false);
            this.onWaitEnd();
        }
    }

    /**
     * 等待结束后要做的回调
     */
    protected void onWaitEnd() {
    }

    
    /**
     * 是否已被终止
     * 
     * @return
     */
    public boolean isStopped() {
        return stopped;
    }
}
