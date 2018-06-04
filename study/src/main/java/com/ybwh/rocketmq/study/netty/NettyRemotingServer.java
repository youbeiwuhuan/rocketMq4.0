package com.ybwh.rocketmq.study.netty;

import java.net.InetSocketAddress;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ybwh.rocketmq.study.netty.conf.NettyServerConfig;
import com.ybwh.rocketmq.study.netty.conn.NettyConnetManageHandler;
import com.ybwh.rocketmq.study.netty.decoder.MyDecoder;
import com.ybwh.rocketmq.study.netty.encoder.MyEncoder;
import com.ybwh.rocketmq.study.netty.util.RemotingUtil;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;

/**
 * 服务端：心跳、编码、解码
 *
 */
public class NettyRemotingServer extends NettyRemotingAbstract {
	private static final Logger log = LoggerFactory.getLogger(NettyRemotingServer.class);
	private final NettyServerConfig nettyServerConfig;
	/**
	 * 这四个是netty性能配置
	 */
	private final ServerBootstrap serverBootstrap;
	private final EventLoopGroup eventLoopGroupSelector;
	private final EventLoopGroup eventLoopGroupBoss;
	private int port = 0;

	/**
	 * 以下是根据业务来配置
	 */
	private DefaultEventExecutorGroup defaultEventExecutorGroup;

	public NettyRemotingServer(NettyServerConfig nettyServerConfig) {
		this.nettyServerConfig = nettyServerConfig;
		this.serverBootstrap = new ServerBootstrap();

		this.eventLoopGroupBoss = new NioEventLoopGroup(1, new ThreadFactory() {
			private AtomicInteger threadIndex = new AtomicInteger(0);

			@Override
			public Thread newThread(Runnable r) {
				return new Thread(r, String.format("NettyBoss_%d", this.threadIndex.incrementAndGet()));
			}
		});

		if (RemotingUtil.isLinuxPlatform() // linux 系统用原生的封装
				&& nettyServerConfig.isUseEpollNativeSelector()) {
			this.eventLoopGroupSelector = new EpollEventLoopGroup(nettyServerConfig.getServerSelectorThreads(),
					new ThreadFactory() {
						private AtomicInteger threadIndex = new AtomicInteger(0);
						private int threadTotal = nettyServerConfig.getServerSelectorThreads();

						@Override
						public Thread newThread(Runnable r) {
							return new Thread(r, String.format("NettyServerEPOLLSelector_%d_%d", threadTotal,
									this.threadIndex.incrementAndGet()));
						}
					});
		} else {
			this.eventLoopGroupSelector = new NioEventLoopGroup(nettyServerConfig.getServerSelectorThreads(),
					new ThreadFactory() {
						private AtomicInteger threadIndex = new AtomicInteger(0);
						private int threadTotal = nettyServerConfig.getServerSelectorThreads();

						@Override
						public Thread newThread(Runnable r) {
							return new Thread(r, String.format("NettyServerNIOSelector_%d_%d", threadTotal,
									this.threadIndex.incrementAndGet()));
						}
					});
		}

	}

	public void start() {
		this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(nettyServerConfig.getServerWorkerThreads(),
				new ThreadFactory() {

					private AtomicInteger threadIndex = new AtomicInteger(0);

					@Override
					public Thread newThread(Runnable r) {
						return new Thread(r, "NettyServerCodecThread_" + this.threadIndex.incrementAndGet());
					}
				});

		ServerBootstrap childHandler = this.serverBootstrap.group(this.eventLoopGroupBoss, this.eventLoopGroupSelector)
				.channel(NioServerSocketChannel.class).option(ChannelOption.SO_BACKLOG, 1024)
				.option(ChannelOption.SO_REUSEADDR, true).option(ChannelOption.SO_KEEPALIVE, false)
				.childOption(ChannelOption.TCP_NODELAY, true)
				.option(ChannelOption.SO_SNDBUF, nettyServerConfig.getServerSocketSndBufSize())
				.option(ChannelOption.SO_RCVBUF, nettyServerConfig.getServerSocketRcvBufSize())
				.localAddress(new InetSocketAddress(this.nettyServerConfig.getListenPort()))
				.childHandler(new ChannelInitializer<SocketChannel>() {
					@Override
					public void initChannel(SocketChannel ch) throws Exception {
						ch.pipeline().addLast(
								/**
								 * 这里指定线程池，下面5个handler均会用此处指定线程池,不会用selector和boss线程池;
								 * 好处就是不会导致业务处理太耗时占用大量了线程而使得IO缺少线程被阻断.
								 */
								defaultEventExecutorGroup, 
								new MyEncoder(), // 编码
								new MyDecoder(), // 解码
								new IdleStateHandler(0, 0, nettyServerConfig.getServerChannelMaxIdleTimeSeconds()), // 心跳
								new NettyConnetManageHandler(), // 连接管理
								new NettyServerHandler());// 这里才是真正的业务处理
					}
				});

		if (nettyServerConfig.isServerPooledByteBufAllocatorEnable()) {
			childHandler.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
		}

		try {
			ChannelFuture sync = this.serverBootstrap.bind().sync();
			InetSocketAddress addr = (InetSocketAddress) sync.channel().localAddress();
			this.port = addr.getPort();

		} catch (InterruptedException e1) {
			throw new RuntimeException("this.serverBootstrap.bind().sync() InterruptedException", e1);
		}
	}

	public void shutdown() {
		this.eventLoopGroupBoss.shutdownGracefully();
		this.eventLoopGroupSelector.shutdownGracefully();

		if (this.defaultEventExecutorGroup != null) {
			this.defaultEventExecutorGroup.shutdownGracefully();
		}
	}

}
