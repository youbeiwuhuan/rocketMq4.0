package com.ybwh.rocketmq.study.netty.encoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class MyEncoder extends MessageToByteEncoder<I>{

	@Override
	protected void encode(ChannelHandlerContext ctx, I msg, ByteBuf out) throws Exception {
		// TODO Auto-generated method stub
		
	}

}
