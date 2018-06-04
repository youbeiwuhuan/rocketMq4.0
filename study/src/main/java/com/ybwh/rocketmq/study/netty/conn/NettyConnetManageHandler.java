package com.ybwh.rocketmq.study.netty.conn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ybwh.rocketmq.study.netty.NettyRemotingServer;
import com.ybwh.rocketmq.study.netty.util.RemotingHelper;
import com.ybwh.rocketmq.study.netty.util.RemotingUtil;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

/**
 * netty连接管理
 *
 */
public class NettyConnetManageHandler extends ChannelDuplexHandler {
	private static final Logger log = LoggerFactory.getLogger(NettyConnetManageHandler.class);
	
	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
		log.info("NETTY SERVER PIPELINE: channelRegistered {}", remoteAddress);
		super.channelRegistered(ctx);
	}

	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
		log.info("NETTY SERVER PIPELINE: channelUnregistered, the channel[{}]", remoteAddress);
		super.channelUnregistered(ctx);
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
		log.info("NETTY SERVER PIPELINE: channelActive, the channel[{}]", remoteAddress);
		super.channelActive(ctx);

		if (NettyRemotingServer.this.channelEventListener != null) {
			NettyRemotingServer.this
					.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remoteAddress.toString(), ctx.channel()));
		}
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
		log.info("NETTY SERVER PIPELINE: channelInactive, the channel[{}]", remoteAddress);
		super.channelInactive(ctx);

		if (NettyRemotingServer.this.channelEventListener != null) {
			NettyRemotingServer.this
					.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress.toString(), ctx.channel()));
		}
	}

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		if (evt instanceof IdleStateEvent) {
			IdleStateEvent evnet = (IdleStateEvent) evt;
			if (evnet.state().equals(IdleState.ALL_IDLE)) {
				final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
				log.warn("NETTY SERVER PIPELINE: IDLE exception [{}]", remoteAddress);
				RemotingUtil.closeChannel(ctx.channel());
				if (NettyRemotingServer.this.channelEventListener != null) {
					NettyRemotingServer.this.putNettyEvent(
							new NettyEvent(NettyEventType.IDLE, remoteAddress.toString(), ctx.channel()));
				}
			}
		}

		ctx.fireUserEventTriggered(evt);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
		log.warn("NETTY SERVER PIPELINE: exceptionCaught {}", remoteAddress);
		log.warn("NETTY SERVER PIPELINE: exceptionCaught exception.", cause);

		if (NettyRemotingServer.this.channelEventListener != null) {
			NettyRemotingServer.this.putNettyEvent(
					new NettyEvent(NettyEventType.EXCEPTION, remoteAddress.toString(), ctx.channel()));
		}

		RemotingUtil.closeChannel(ctx.channel());
	}

}
