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
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.MappedFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 为消息查询提供了一种通过key或时间区间来查询消息的方法
 * 
 * <pre>
 * 总体结构
 * +-----------------+----------------------------------+------------------------------------+
 * |     Header      |        slot table                |        index linked list           |
 * +-----------------+----------------------------------+------------------------------------+
 * |----- 40B -------|-------  4*500W  -----------------|---------- 20*2000W  ---------------|
 * 
 * Header 结构 {@link IndexHeader}
 * 
 * 
 * index linked list 中节点结构
 *      4B                  8B                   4B                      4B
 * +-----------------+------------------+-----------------------+---------------------+
 * |   keyHash       |    phyOffset     |   timeDiff            |   slotValue         |
 * +-----------------+------------------+-----------------------+---------------------+
 *  keyHash: 4位，int值，key的hash值
 *  phyOffset：8位，long值，索引消息commitLog偏移量
 *  timeDiff: 4位，int值，索引存储的时间与IndexHeader的beginTimestamp时间差(这是为了节省空间)
 *  slotValue:4位，int值，索引对应slot的上一条索引的下标(据此完成一条向老记录的链表)
 * </pre>
 *
 */
public class IndexFile {
	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
	/**
	 * 每一个槽信息含4字节，即该slot对应的count数
	 */
	private static final int hashSlotSize = 4;
	/**
	 * 每一个索引信息占据20字节
	 */
	private static final int indexSize = 20;
	/**
	 * invalidIndex是0表示无效索引下标
	 */
	private static final int invalidIndex = 0;

	/**
	 * hash槽的个数，默认5百万
	 */
	private final int hashSlotNum;
	/**
	 * 最多记录的索引条数,默认2千万
	 */
	private final int indexNum;

	private final MappedFile mappedFile;

	/**
	 * 实际上是mappedFile的fileChannel
	 */
	private final FileChannel fileChannel;
	/**
	 * 实际上是mappedFile的mappedByteBuffer,没有用到
	 */
	private final MappedByteBuffer mappedByteBuffer;

	/**
	 * 头部结构
	 */
	private final IndexHeader indexHeader;

	public IndexFile(final String fileName, final int hashSlotNum, final int indexNum, final long endPhyOffset,
			final long endTimestamp) throws IOException {
		int fileTotalSize = IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
		this.mappedFile = new MappedFile(fileName, fileTotalSize);
		this.fileChannel = this.mappedFile.getFileChannel();
		this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
		this.hashSlotNum = hashSlotNum;
		this.indexNum = indexNum;

		ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
		this.indexHeader = new IndexHeader(byteBuffer);

		if (endPhyOffset > 0) {
			this.indexHeader.setBeginPhyOffset(endPhyOffset);
			this.indexHeader.setEndPhyOffset(endPhyOffset);
		}

		if (endTimestamp > 0) {
			this.indexHeader.setBeginTimestamp(endTimestamp);
			this.indexHeader.setEndTimestamp(endTimestamp);
		}
	}

	public String getFileName() {
		return this.mappedFile.getFileName();
	}

	public void load() {
		this.indexHeader.load();
	}

	public void flush() {
		long beginTime = System.currentTimeMillis();
		if (this.mappedFile.hold()) {
			this.indexHeader.updateByteBuffer();
			this.mappedByteBuffer.force();
			this.mappedFile.release();
			log.info("flush index file eclipse time(ms) " + (System.currentTimeMillis() - beginTime));
		}
	}

	/**
	 * 
	 * 是否写满了，就是已经写的索引数是否超过了指定的索引数（2千万）
	 * 
	 * @return
	 */
	public boolean isWriteFull() {
		return this.indexHeader.getIndexCount() >= this.indexNum;
	}

	public boolean destroy(final long intervalForcibly) {
		return this.mappedFile.destroy(intervalForcibly);
	}

	/**
	 * 存放一条索引消息,更新IndexHeader，slotTable，Index Linked List三个记录
	 * 
	 * @param key
	 *            topic-key值
	 * @param phyOffset
	 *            物理偏移
	 * @param storeTimestamp
	 *            时间戳
	 * @return
	 */
	public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
		if (this.indexHeader.getIndexCount() < this.indexNum) {
			int keyHash = indexKeyHashMethod(key);
			int slotPos = keyHash % this.hashSlotNum;
			int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

			FileLock fileLock = null;

			try {

				// fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize,
				// false);
				int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
				if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
					slotValue = invalidIndex;
				}

				long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

				timeDiff = timeDiff / 1000;

				if (this.indexHeader.getBeginTimestamp() <= 0) {
					timeDiff = 0;
				} else if (timeDiff > Integer.MAX_VALUE) {
					timeDiff = Integer.MAX_VALUE;
				} else if (timeDiff < 0) {
					timeDiff = 0;
				}

				int absIndexPos = IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
						+ this.indexHeader.getIndexCount() * indexSize;

				this.mappedByteBuffer.putInt(absIndexPos, keyHash);
				this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
				this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
				this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);

				this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

				if (this.indexHeader.getIndexCount() <= 1) {
					this.indexHeader.setBeginPhyOffset(phyOffset);
					this.indexHeader.setBeginTimestamp(storeTimestamp);
				}

				this.indexHeader.incHashSlotCount();
				this.indexHeader.incIndexCount();
				this.indexHeader.setEndPhyOffset(phyOffset);
				this.indexHeader.setEndTimestamp(storeTimestamp);

				return true;
			} catch (Exception e) {
				log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
			} finally {
				if (fileLock != null) {
					try {
						fileLock.release();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		} else {
			log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
					+ "; index max num = " + this.indexNum);
		}

		return false;
	}

	/**
	 * 获取key的hash值(int)，转成绝对值
	 * 
	 * @param key
	 * @return
	 */
	public int indexKeyHashMethod(final String key) {
		int keyHash = key.hashCode();
		int keyHashPositive = Math.abs(keyHash);
		if (keyHashPositive < 0)
			keyHashPositive = 0;
		return keyHashPositive;
	}

	public long getBeginTimestamp() {
		return this.indexHeader.getBeginTimestamp();
	}

	public long getEndTimestamp() {
		return this.indexHeader.getEndTimestamp();
	}

	public long getEndPhyOffset() {
		return this.indexHeader.getEndPhyOffset();
	}

	/**
	 * 
	 * [begin,end]时间与 [indexHeader.getBeginTimestamp(),
	 * this.indexHeader.getEndTimestamp()]时间有重叠
	 * 
	 * @param begin
	 * @param end
	 * @return
	 */
	public boolean isTimeMatched(final long begin, final long end) {
		boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
		result = result
				|| (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
		result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
		return result;
	}

	/**
	 * 根据key找到slot对应的整个列表，找到keyHash一样且存储时间在给定的[begin,end]范围内的索引列表（最多maxNum条，从新到旧），
	 * 返回他们的物理偏移
	 *
	 * @param phyOffsets
	 *            物理偏移列表,用于返回
	 * @param key
	 *            关键字
	 * @param maxNum
	 *            phyOffsets最多保存这么多条记录就返回
	 * @param begin
	 *            开始时间戳
	 * @param end
	 *            结束时间戳
	 * @param lock:没有用
	 */
	public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum, final long begin,
			final long end, boolean lock) {
		if (this.mappedFile.hold()) {
			int keyHash = indexKeyHashMethod(key);
			int slotPos = keyHash % this.hashSlotNum;
			int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

			FileLock fileLock = null;
			try {
				if (lock) {
					// fileLock = this.fileChannel.lock(absSlotPos,
					// hashSlotSize, true);
				}

				int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
				// if (fileLock != null) {
				// fileLock.release();
				// fileLock = null;
				// }

				if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
						|| this.indexHeader.getIndexCount() <= 1) {
					// TODO NOTFOUND
				} else {
					for (int nextIndexToRead = slotValue;;) {
						if (phyOffsets.size() >= maxNum) {
							break;
						}

						int absIndexPos = IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
								+ nextIndexToRead * indexSize;

						int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
						long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

						long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
						int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

						if (timeDiff < 0) {
							break;
						}

						timeDiff *= 1000L;

						long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
						boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

						if (keyHash == keyHashRead && timeMatched) {
							phyOffsets.add(phyOffsetRead);
						}

						if (prevIndexRead <= invalidIndex || prevIndexRead > this.indexHeader.getIndexCount()
								|| prevIndexRead == nextIndexToRead || timeRead < begin) {
							break;
						}

						nextIndexToRead = prevIndexRead;
					}
				}
			} catch (Exception e) {
				log.error("selectPhyOffset exception ", e);
			} finally {
				if (fileLock != null) {
					try {
						fileLock.release();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}

				this.mappedFile.release();
			}
		}
	}
}
