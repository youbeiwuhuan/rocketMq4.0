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
package org.apache.rocketmq.store;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 实现了一个可回收资源的逻辑，由于refCount初始化为1，所以实现了一个可独占可回收的资源逻辑
 */
public abstract class ReferenceResource {
	
	//available为true且refCount大于0表示资源可用
    /**
     * 引用数量（大于0表示资源被占用）
     */
    protected final AtomicLong refCount = new AtomicLong(1);
    /**
     * 资源是否可被申请占用
     */
    protected volatile boolean available = true;
    /**
     * 是否回收完成
     */
    protected volatile boolean cleanupOver = false;
    /**
     * 首次关闭的时间戳（用于超时强制关闭）
     */
    private volatile long firstShutdownTimestamp = 0;

    /**
     * 是否可以独占该资源，该方法作用是相当于一个非阻塞的锁，任何时候最多让一个线程占有该资源
     * 
     * @return
     */
    public synchronized boolean hold() {
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    /**
     * 资源是否可被申请来使用
     * 
     * @return
     */
    public boolean isAvailable() {
        return this.available;
    }

    /**
     * 关闭
     * 
     * @param intervalForcibly 多久后强制关闭
     */
    public void shutdown(final long intervalForcibly) {
        if (this.available) {//可访问直接关闭
            this.available = false;
            this.firstShutdownTimestamp = System.currentTimeMillis();
            this.release();
        } else if (this.getRefCount() > 0) {
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {//关闭超时（不可访问但引用数大于0且超时未关闭）
                this.refCount.set(-1000 - this.getRefCount());//强行将资源设置为不可用（引用数设为负数，一般引用数不会超过1000）
                this.release();
            }
        }
    }

    /**
     * 释放占用，回收资源
     */
    public void release() {
        long value = this.refCount.decrementAndGet();
        if (value > 0)//引用数大于0则不回收
            return;

        synchronized (this) {

            this.cleanupOver = this.cleanup(value);
        }
    }

    /**
     * 获取占用数目
     * @return
     */
    public long getRefCount() {
        return this.refCount.get();
    }

    /**
     * 回收资源
     * 
     * @param currentRef 当前引用数
     * @return
     */
    public abstract boolean cleanup(final long currentRef);

    /**
     * 是否已经回收完成
     * 
     * @return
     */
    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
