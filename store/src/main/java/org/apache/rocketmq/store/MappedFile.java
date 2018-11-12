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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;

import sun.nio.ch.DirectBuffer;

/**
 * 包含了具体的文件信息，包括文件路径，文件名，文件起始偏移，写位移，读位移等等信息，同时使用了虚拟内存映射来提高IO效率；
 * 
 * 上层只有{@link org.apache.rocketmq.store.index.IndexFile} 和 {@link MappedFileQueue} 依赖此类
 * 
 * 该类既有FileChannel的写和刷盘，也有MappedByteBuffer的写和刷盘。
 * 当申请到了写缓存（writeBuffer != null）,则用FileChannel,若没有申请到写缓存则用MappedByteBuffer
 * 
 *
 */
public class MappedFile extends ReferenceResource {
    /**
     *  操作系统内存页大小（4k）
     */
    public static final int OS_PAGE_SIZE = 1024 * 4;
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 映射文件所占虚拟内存总大小  （所有映射文件大小的总和）
     */
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    /**
     * 映射文件总数
     */
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);
    
    
    /**
     * 当前写文件的位置，初始值0
     */
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);
    //ADD BY ChenYang
    /**
     * 当前提交位置，初始值0
     */
    protected final AtomicInteger committedPosition = new AtomicInteger(0);
    /**
     * 当前刷盘位置，初始值0
     */
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    /**
     * 映射文件的大小
     */
    protected int fileSize;
    
    
    /**
     * 映射的fileChannel对象
     */
    protected FileChannel fileChannel;
    /**
     * 写缓存
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     */
    protected ByteBuffer writeBuffer = null;
    
    /**
     * //池对象，init函数中赋值
     */
    protected TransientStorePool transientStorePool = null;
    /**
     * 映射的文件名，一般为20位数字,代表这个文件开始时的offset
     */
    private String fileName;
    /**
     * 文件偏移量 (文件中第一个消息在消息队列的偏移量)
     */
    private long fileFromOffset;
    /**
     * 映射的文件
     */
    private File file;
    /**
     * 映射的内存对象
     */
    private MappedByteBuffer mappedByteBuffer;
    /**
     * 最后一条消息保存时间
     */
    private volatile long storeTimestamp = 0;
    /**
     * 是不是刚刚创建的
     */
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    /**
     * 不带写缓存的对象构造器
     * 
     * @param fileName 文件名
     * @param fileSize 文件大小
     * 
     * @throws IOException
     */
    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    /**
     * 带写缓存的对象构造器
     * 
     * @param fileName 文件名
     * @param fileSize 文件大小
     * @param transientStorePool 缓存池
     * 
     * @throws IOException
     */
    public MappedFile(final String fileName, final int fileSize, 
    		final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    /**
     * 确认目录存在，不存在则创建一个
     * 
     * @param dirName 目录路径
     */
    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    
    /*以下4个方法是解决MappedByteBuffer未提供unmap方法而不能立即释放映射内存问题,
	    主要逻辑是调用 sun.misc.Cleaner的clean方法
	*/
    /**
     * 释放映射内存;
     * 参考FileChannelImpl的unmap方法来实现的，FileChannelImpl的unmap方法如下
     * <pre>
     * private static void unmap(MappedByteBuffer bb) {  
		    Cleaner cl = ((DirectBuffer)bb).cleaner();  
		    if (cl != null)  
		        cl.clean();  
		}  
     * </pre>
     * 实际两步：
     * 1.调用DirectBuffer的cleaner()方法获取sun.misc.Cleaner对象()
     * 2.调用sun.misc.Cleaner对象对象的clean()方法释放内存
     * 
     * 
     * @param buffer 一定是DirectBuffer对象，否则啥都不做
     * 
     */
    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)//判断是否DirectBuffer对象，不是则不释放
            return;
        /**
         * 同时实现ByteBuffer和DirectBuffer的只有DirectByteBuffer，
         * 而MappedByteBuffer只有1个实现类DirectByteBuffer,所以只要代码走到这里，肯定是MappedByteBuffer对象
         * 
         * 
         * 
         * viewed(buffer)获取真实的buffer
         * invoke(viewed(buffer), "cleaner")  获取ByteBuffer对应的Cleaner对象
         */
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
        
    }
    
    
    
    
    /**
     * 跳过权限检查，调用对象上的方法
     * 
     * @param target 对象
     * @param methodName 方法名
     * @param args 方法的参数类型列表
     * @return 执行后返回值
     */
    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        /**
         * 调用sun.XXX.XXX包下的类的方法需要权限,跳过权限检查
         */
    	return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    /**
     * 获取对象上的方法
     * 
     * 
     * @param target 对象
     * @param methodName 方法名
     * @param args 方法参数类型列表
     * @return
     * @throws NoSuchMethodException
     */
    private static Method method(Object target, String methodName, Class<?>[] args)
        throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    /**
     * 找到关联这块内存的找到最底层的ByteBuffer对象
     * 
     * @param buffer
     * @return
     */
    private static ByteBuffer viewed(ByteBuffer buffer) {
    	/**
         * 对象有attachment方法（DirectByteBuffer有）则invoke调用attachment方法，没有则invoke调用viewedBuffer方法(这个我还没找到哪个类有此方法)，
         * 一级级递归调用，直到invoke返回null,则找到最底层的关联对象
         */
    	
    	
        String methodName = "viewedBuffer";
        
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }
    /*
     * clean相关的4个方法结束
     */
    
    
    
    

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    /**
     * @param fileName 文件名称
     * @param fileSize 文件大小
     * @param transientStorePool 缓存池
     * @throws IOException
     */
    public void init(final String fileName, final int fileSize, final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        this.writeBuffer = transientStorePool.borrowBuffer();//从缓冲池 取一块内存作写缓存，这里writeBuffer可能为空
        this.transientStorePool = transientStorePool;
    }

    /**
     * 
     * 
     * 
     * 初始化映射文件
     * 
     * <p>做了3件事：
     * 1.获取文件在消息队列中偏移量
     * 2.确认文件父目录存在，不存在则创建一个
     * 3.创建文件映射
     * </p>
     * 
     * @param fileName 文件名称
     * @param fileSize 文件大小
     * @throws IOException
     */
    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        //1.获取文件在消息队列中偏移量
        this.fileFromOffset = Long.parseLong(this.file.getName());//文件名称就是偏移量
        /**
         * 是否映射成功
         */
        boolean ok = false;

        //2.确认文件父目录存在，不存在则创建一个
        ensureDirOK(this.file.getParent());

        try {
        	
        	/*
        	 * 初始化文件名，大小
            * 设置fileFromOffset代表文件对应的偏移量
            * 得到fileChannel，mappedByteBuffer 得到IO相关对象
            * 计数TOTAL_MAPPED_VIRTUAL_MEMORY,TOTAL_MAPPED_FILES更新
            * 
            */
        	
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            
            //所有映射文件总大小增加、映射文件数量加1
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
            
        } catch (FileNotFoundException e) {
            log.error("create file channel " + this.fileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            log.error("map file " + this.fileName + " Failed. ", e);
            throw e;
        } finally {
        	//映射失败但文件通道创建成功则关闭文件通道。  （资源回收非常严谨）
            if (!ok && this.fileChannel != null) {
            	/**
            	 * 文件通道唯一，所以关闭文件通道即可，无需再关闭RandomAccessFile
            	 */
                this.fileChannel.close();
            }
        }
    }

    /**
     * 文件最后修改的时间
     * 
     * 
     * @return
     */
    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    /**
     * 文件大小
     * 
     * @return
     */
    public int getFileSize() {
        return fileSize;
    }

    /**
     * 文件通道
     * @return
     */
    public FileChannel getFileChannel() {
        return fileChannel;
    }
    
    

    /**
     * 提供给commitlog使用的，传入消息内容，然后CommitLog按照规定的格式构造二进制信息并顺序写
     * 
     * @param msg MessageExtBrokerInner或者MessageExtBatch，具体的写操作是在回调类AppendMessageCallback的doAppend方法进行
     * @param cb
     * @return
     */
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
        assert msg != null;
        assert cb != null;
        //找出当前要的写入位置
        int currentPos = this.wrotePosition.get();

        //当前位置小于文件大小则可以尝试写入
        if (currentPos < this.fileSize) {
        	/**
        	 * 如果有 写缓存  则写入 写缓存
        	 */
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            
            AppendMessageResult result =
                cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, msg);
            
            this.wrotePosition.addAndGet(result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }

        log.error("MappedFile.appendMessage return null, wrotePosition: " + currentPos + " fileSize: "
            + this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    /**
     * 文件偏移量 (文件中第一个消息在消息队列的偏移量)
     */
    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

   
    /**
     * 真正顺序写入文件操作
     * 
     * @param data
     * @return
     */
    public boolean appendMessage(final byte[] data) {
    	//找出当前要的写入位置
        int currentPos = this.wrotePosition.get();
        //如果当前位置加上要写入的数据大小小于等于文件大小，则说明剩余空间足够写入。
        if ((currentPos + data.length) <= this.fileSize) {
            try {
            	//则由内存对象 mappedByteBuffer 创建一个指向同一块内存的
                //ByteBuffer 对象，并将内存对象的写入指针指向写入位置；然后将该二进制信
                //息写入该内存对象，同时将 wrotePostion 值增加消息的大小；
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * 刷盘 MappedFileQueue调用
     * 
     * @param flushLeastPages 最小刷盘页数
     * @return The current flushed position
     */
    public int flush(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = getReadPosition();

                try {
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    /**
     * 消息提交操作
     * 
     *  （1）首先判断文件是否已经写满类，即wrotePosition等于fileSize，若写满则进行刷盘操作 
	 *	（2）检测内存中尚未刷盘的消息页数是否大于最小刷盘页数，不够页数也暂时不刷盘。 
	 *	（3）MappedFile的父类是ReferenceResource，该父类作用是记录MappedFile中的引用次数，
	 *		为正表示资源可用，刷盘前加一，然后将wrotePosotion的值赋给committedPosition，再减一。
	 *  （4） 释放掉内存映射文件
     * 
     * @param commitLeastPages 提交的最少页数
     * @return 提交位置
     */
    public int commit(final int commitLeastPages) {
    	
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePosition.get();
        }
        if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                commit0(commitLeastPages);
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }

        return this.committedPosition.get();
    }

    /**
     * 真正提交操作
     * @param commitLeastPages 提交的最少页数
     */
    protected void commit0(final int commitLeastPages) {
        int writePos = this.wrotePosition.get();
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - this.committedPosition.get() > 0) {
            try {
                ByteBuffer byteBuffer = writeBuffer.slice();
                byteBuffer.position(lastCommittedPosition);
                byteBuffer.limit(writePos);
                this.fileChannel.position(lastCommittedPosition);
                this.fileChannel.write(byteBuffer);
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    /**
     * 是否可以刷盘
     * 
     * @param flushLeastPages 刷盘的最少页数
     * @return 
     */
    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.flushedPosition.get();
        int write = getReadPosition();

        if (this.isFull()) {
            return true;
        }

        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }

    /**
     * 判断是否能提交
     * 
     * @param commitLeastPages
     * @return
     */
    protected boolean isAbleToCommit(final int commitLeastPages) {
        int flush = this.committedPosition.get();
        int write = this.wrotePosition.get();

        if (this.isFull()) {
            return true;
        }

        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > flush;
    }

    /**
     * @return
     */
    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    /**
     * 判断文件是否已写满,当前写入位置等于文件大小时写满
     * 
     * @return
     */
    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    /**
     * 随机读
     * @param pos
     * @param size
     * @return
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {

            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

   
    /**
     * 随机读
     * @param pos
     * @return
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have not shutdown, stop unmaping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have cleanup, do not do it again.");
            return true;
        }

        //关闭内存映射文件
        clean(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                    + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                    + this.getFlushedPosition() + ", "
                    + UtilAll.computeEclipseTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
            System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
        	/**
        	 * 这里没有其他进程来共享mappedByteBuffer，不用考虑copyOnWrite问题，所以直接mlock即可
        	 */
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
