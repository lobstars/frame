package com.lobstar.base.role.master;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpHeaders.Values;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HTTPMissionWindow {
	private static final Logger logger = LoggerFactory.getLogger(HTTPMissionWindow.class);
	
	private int httpServerPort = 8087;
	
	public HTTPMissionWindow() throws Exception{
		EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                                @Override
                                public void initChannel(SocketChannel ch) throws Exception {
                                    // server端发送的是httpResponse，所以要使用HttpResponseEncoder进行编码
                                    ch.pipeline().addLast(new HttpResponseEncoder());
                                    // server端接收到的是httpRequest，所以要使用HttpRequestDecoder进行解码
                                    ch.pipeline().addLast(new HttpRequestDecoder());
                                    ch.pipeline().addLast(new HttpServerInboundHandler());
                                }
                            }).option(ChannelOption.SO_BACKLOG, 128) 
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = b.bind(httpServerPort).sync();

            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
	}
	
	
	
	private class HttpServerInboundHandler extends ChannelInboundHandlerAdapter {


		    @Override
		    public void channelRead(ChannelHandlerContext ctx, Object msg)
		            throws Exception {
		    	HttpRequest request = null;
		        if (msg instanceof HttpRequest) {
		            request = (HttpRequest) msg;		           
		            String uri =  request.uri();
		            System.out.println("Uri:" + uri);
		        }
		        if (msg instanceof HttpContent) {
		            HttpContent content = (HttpContent) msg;
		            ByteBuf buf = content.content();
		            System.out.println(buf.toString(io.netty.util.CharsetUtil.UTF_8));
		            buf.release();

		            String res = "I am OK";
		            FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
		            		HttpResponseStatus.OK, Unpooled.wrappedBuffer(res.getBytes("UTF-8")));
		            response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain");
		            response.headers().set(HttpHeaders.Names.CONTENT_LENGTH,
		                    response.content().readableBytes());
		            if (HttpHeaders.isKeepAlive(request)) {
		                response.headers().set(HttpHeaders.Names.CONNECTION, Values.KEEP_ALIVE);
		            }
		            ctx.write(response);
		            ctx.flush();
		        }
		    }

		    @Override
		    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		        ctx.flush();
		    }

		    @Override
		    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		        System.out.println(cause.getMessage());
		        ctx.close();
		    }

	}
	
	public static void main(String[] args) throws Exception{
		new HTTPMissionWindow();
	}
}
