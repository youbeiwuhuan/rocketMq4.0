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

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.util.LibC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

/**
 * 用来缓存日志文件的缓存池
 *
 */
public class TransientStorePool {
	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	/**
	 * 内存池大小
	 */
	private final int poolSize;
	/**
	 * 日志文件大小
	 */
	private final int fileSize;
	/**
	 * 可使用的buffer队列
	 */
	private final Deque<ByteBuffer> availableBuffers;
	/**
	 * 消息存储配置信息
	 */
	private final MessageStoreConfig storeConfig;

	public TransientStorePool(final MessageStoreConfig storeConfig) {
		this.storeConfig = storeConfig;
		this.poolSize = storeConfig.getTransientStorePoolSize();
		this.fileSize = storeConfig.getMapedFileSizeCommitLog();

		/**
		 * 考虑并发性
		 */
		this.availableBuffers = new ConcurrentLinkedDeque<>();
	}

	/**
	 * It's a heavy init method.
	 */
	public void init() {
		for (int i = 0; i < poolSize; i++) {
			ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);

			/**
			 * 调用本地代码锁定缓存页，不让操作系统将byteBuffer所占内存替换到交换分区
			 */
			final long address = ((DirectBuffer) byteBuffer).address();
			Pointer pointer = new Pointer(address);
			LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));

			availableBuffers.offer(byteBuffer);
		}
	}

	/**
	 * 销毁池
	 */
	public void destroy() {
		for (ByteBuffer byteBuffer : availableBuffers) {
			/**
			 * 解锁缓存页
			 */
			final long address = ((DirectBuffer) byteBuffer).address();
			Pointer pointer = new Pointer(address);
			LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
		}
	}

	/**
	 * 归还使用的buffer
	 * 
	 * @param byteBuffer
	 */
	public void returnBuffer(ByteBuffer byteBuffer) {
		byteBuffer.position(0);
		byteBuffer.limit(fileSize);
		this.availableBuffers.offerFirst(byteBuffer);
	}

	/**
	 * 从池中借一个buffer
	 * 
	 * @return
	 */
	public ByteBuffer borrowBuffer() {
		ByteBuffer buffer = availableBuffers.pollFirst();
		if (availableBuffers.size() < poolSize * 0.4) {
			log.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
		}
		return buffer;
	}

	/**
	 * 剩余Buffer数量
	 * 
	 * @return
	 */
	public int remainBufferNumbs() {
		if (storeConfig.isTransientStorePoolEnable()) {
			return availableBuffers.size();
		}
		return Integer.MAX_VALUE;
	}
}
