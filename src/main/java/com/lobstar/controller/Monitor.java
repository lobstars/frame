package com.lobstar.controller;

import java.nio.charset.Charset;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

import com.lobstar.config.Constant;
import com.lobstar.index.QueryTools;
import com.lobstar.utils.Utils;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;

public class Monitor {
//	private ServerBootstrap bootstrap;
//	private static Monitor monitor = new Monitor();
//	private Monitor(){
//		EventLoopGroup parentGroup = new NioEventLoopGroup(Constant.MASTER_TICKET_WINDOW_THREAD_SIZE);
//        EventLoopGroup childGroup = new NioEventLoopGroup(Constant.MASTER_TICKET_WINDOW_THREAD_SIZE);
//        this.bootstrap = new ServerBootstrap();
//        MonitorInitHandler serverHandler = new MonitorInitHandler();
//        bootstrap.group(parentGroup, childGroup).channel(NioServerSocketChannel.class).childHandler(serverHandler)
//                .option(ChannelOption.SO_KEEPALIVE, true);
//	}
//	
//	
//	
//	private class MonitorInitHandler extends ChannelInitializer<SocketChannel> {
//
//		@Override
//		protected void initChannel(SocketChannel ch) throws Exception {
//			ch.pipeline().addLast("encoder", new LengthFieldPrepender(4, false));
//            ch.pipeline().addLast(new MonitorHandler());			
//		}
//	}
//	
//	private class MonitorHandler extends ChannelInboundHandlerAdapter {
//		
//		public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//			ByteBuf byteBuf = (ByteBuf) msg;
//	        ObjectMapper objectMapper = new ObjectMapper();
//	        try {
//
//	            int readableBytes = byteBuf.readableBytes();
//	            byte[] readBytes = new byte[readableBytes];
//	            byteBuf.readBytes(readBytes);
//	            String reciveMsg = new String(readBytes, Charset.forName(Constant.GLOBAL_CHARSET));
//	            @SuppressWarnings("unchecked")
//	            Map<String, Object> data = objectMapper.readValue(reciveMsg, Map.class);
//	            data.put(Constant.INDEX_VISITOR_TIME_SYMBOL, Utils.getSystemTime());
//	        } finally {
//	            byteBuf.release();
//	        }		
//		}
//	}
}
