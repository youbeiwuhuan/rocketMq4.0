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
package org.apache.rocketmq.store.util;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;

public interface LibC extends Library {
	LibC INSTANCE = (LibC) Native.loadLibrary(Platform.isWindows() ? "msvcrt" : "c", LibC.class);

	int MADV_WILLNEED = 3;
	int MADV_DONTNEED = 4;

	int MCL_CURRENT = 1;
	int MCL_FUTURE = 2;
	int MCL_ONFAULT = 4;

	/* sync memory asynchronously */
	int MS_ASYNC = 0x0001;
	/* invalidate mappings & caches */
	int MS_INVALIDATE = 0x0002;
	/* synchronous memory sync */
	int MS_SYNC = 0x0004;

	/**
	 * 系统调用 mlock 家族允许程序在物理内存上锁住它的部分或全部地址空间。 这将阻止Linux 将这个内存页调度到交换空间（swap
	 * space），即使 该程序已有一段时间没有访问这段空间。
	 * <p>
	 * 需注意的是，仅分配内存并调用 mlock
	 * 并不会为调用进程锁定这些内存，因为对应的分页可能是写时复制（copy-on-write）的5。因此，你应该在每个页面中写入一个假的值：
	 * size_t i; size_t page_size = getpagesize (); for (i = 0; i < alloc_size;
	 * i += page_size) memory[i] = 0; 这样针对每个内存分页的写入操作会强制 Linux
	 * 为当前进程分配一个独立、私有的内存页。
	 * </p>
	 * 
	 * @param var1
	 * @param var2
	 * @return
	 */
	int mlock(Pointer var1, NativeLong var2);

	int munlock(Pointer var1, NativeLong var2);

	int madvise(Pointer var1, NativeLong var2, int var3);

	Pointer memset(Pointer p, int v, long len);

	int mlockall(int flags);

	int msync(Pointer p, NativeLong length, int flags);
}
