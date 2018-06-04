package com.ybwh.rocketmq.study.netty;

import java.util.concurrent.atomic.AtomicInteger;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * 全局的入站IO处理器(单例模式)<br/>
 * 做一些统计工作
 *
 */
@Sharable
public class GlobalChannelInboundHandler extends ChannelInboundHandlerAdapter {
	private static volatile GlobalChannelInboundHandler instance;

	private GlobalChannelInboundHandler() {

	}

	public GlobalChannelInboundHandler getInstance() {
		if (null == instance) {
			synchronized (GlobalChannelInboundHandler.class) {
				if (null == instance) {
					instance = new GlobalChannelInboundHandler();
				}
			}

		}

		return instance;

	}

	/**
	 * 统计总连接数
	 */
	private AtomicInteger totalConn = new AtomicInteger(0);

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		totalConn.incrementAndGet();
		ctx.fireChannelInactive();
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		totalConn.decrementAndGet();
		ctx.fireChannelInactive();
	}

}
