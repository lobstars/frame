package com.lobstar.base.role;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;

import com.lobstar.base.exception.MissionException;
import com.lobstar.base.log.XLogger;
import com.lobstar.config.Constant;
import com.lobstar.manage.IWorkerListener;

public class Mission {

    private static final Logger LOG = XLogger.getLogger(Mission.class);

    private String isDone = "false";
    private String isAssign = "true";
    private Map<String, Object> data;

    private Map<String, Object> retMap;

    private String host;
    private int port;
    
    private boolean isSubmit = false;
    private boolean async = false;
    private int timeout = 20000;
    private CountDownLatch latch;

    private NioEventLoopGroup nioEventLoopGroup;
    private Bootstrap bootstrap;
    private ChannelFuture connect;

    private MissionReport report = new MissionReport();

    public Mission(String host, int port) {
    	initVisitor();
        this.host = host;
        this.port = port;
    }
    
    public Mission() {
    	initVisitor();
	}
    
    private void initVisitor()
    {
    	nioEventLoopGroup = new NioEventLoopGroup();
        bootstrap = new Bootstrap();
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new ClientHandler());
        bootstrap.group(nioEventLoopGroup);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, false);
    }
    
    public Mission connect(String host, int port) {
    	this.host = host;
        this.port = port;
        return this;
    }
    
    public Mission addParam(String key,Object value) {
    	if(data == null) {
    		data = new HashMap<String, Object>();
    	}
    	data.put(key, value);
    	return this;
    }
    
    public Mission setTimeout(int timeout) {
    	this.timeout = timeout;
    	return this;
    }

    public String getIsDone() {
        return isDone;
    }

    public void setIsDone(String isDone) {
        this.isDone = isDone;
    }

    public String getIsAssign() {
        return isAssign;
    }

    public void setIsAssign(String isAssign) {
        this.isAssign = isAssign;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }
    
    public Mission submit() throws InterruptedException {
        if (data == null) {
            data = new HashMap<String, Object>();
        }
        data.put(Constant.WORK_ASSIGN_SYMBOL, isAssign);
        data.put(Constant.WORK_DONE_SYMBOL, isDone);
        data.put(Constant.VISITOR_TIMEZONE_SYMBOL, Calendar.getInstance().getTimeZone().getID());
        connect = bootstrap.connect(new InetSocketAddress(host, port)).sync();
        latch = new CountDownLatch(1);
        isSubmit = true;
        return this;
    }

    public MissionReport reportGet() throws Exception{
    	if(latch.await(timeout, TimeUnit.MILLISECONDS)) {
    		close();
    		return report;
    	}
    	close();
    	throw new MissionException("report timeout");
    	
    }

    public void close() {
        if (connect != null) {
            try {
                connect.channel().close().sync();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (nioEventLoopGroup != null) {
            nioEventLoopGroup.shutdownGracefully();
        }
    }

    public void getResponse(int tryNum, int sleeptime, IWorkerListener<Mission> listener) {
        new SeekResponse( tryNum, sleeptime, listener, this).start();

    }

    private class SeekResponse extends Thread {

        private int tryNum;
        private int sleeptime;
        private IWorkerListener<Mission> listener;
        private Mission visitor;

        public SeekResponse(int tryNum, int sleeptime, IWorkerListener<Mission> listener, Mission visitor) {
            this.tryNum = tryNum;
            this.sleeptime = sleeptime;
            this.listener = listener;
            this.visitor = visitor;
        }

        @Override
        public void run() {
            Object ret = getResponse(tryNum, sleeptime);
            listener.responseVisitor(visitor, ret);
            close();
        }
    }
    
    private class ClientHandler extends ChannelInitializer<SocketChannel> {

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
        	ch.pipeline().addLast("decode", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
            ch.pipeline().addLast(new SendDataHandler());
        }
    }

    public Object getResponse(int tryNum, int sleeptime) {
    	Map<String,Object> response = null;
        for (int i = 0; i < tryNum; i++) {
            if (retMap != null) {
            	response = retMap;
            	break;
            }
            try {
                Thread.sleep(sleeptime);
            } catch (InterruptedException e) {
                LOG.error(e.getMessage(),e);
            }
        }
        close();
        return response;
    }
    
    public Object getReturnValue() throws Exception {
    	if(latch.await(timeout, TimeUnit.MILLISECONDS)) {
    		return retMap.get(Constant.WORK_RESPONSE_SYMBOL);    		    		
    	}else {
    		throw new MissionException("report timeout");
    	}
    }
    
    public Object getReturnError() throws Exception{
    	if(latch.await(timeout, TimeUnit.MILLISECONDS)) {
    		return retMap.get(Constant.WORK_EXCEPTION);    		
    	}else{
    		throw new MissionException("report timeout");
    	}
    }
        
    @Deprecated
    public Object getReturnValue(int tryNum, int sleeptime) {
    	Object response = getResponse(tryNum, sleeptime);
    	Map<String,Object> returnMap = (Map<String,Object>)response;
    	if(returnMap != null) {
    		return returnMap.get(Constant.WORK_RESPONSE_SYMBOL);    		
    	}
    	return null;
    }

    private class SendDataHandler extends ChannelInboundHandlerAdapter {
        ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            String jsonData = objectMapper.writeValueAsString(data);
            //            JSONObject jsonData = JSONObject.fromObject(data);
            byte[] bytes = jsonData.getBytes(Charset.forName("utf-8"));
            ByteBuf msg = ctx.alloc().buffer(bytes.length);
            msg.writeBytes(bytes);
            ctx.channel().writeAndFlush(msg);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ByteBuf ret = (ByteBuf) msg;
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                int len = ret.readableBytes();
                byte[] by = new byte[len];
                ret.readBytes(by);
                @SuppressWarnings("unchecked")
                Map<String, Object> data = objectMapper.readValue(
                        new String(by, Charset.forName(Constant.GLOBAL_CHARSET)), Map.class);
                retMap = data;
                report.setComplete(true);
                report.setResult((Map)retMap.get(Constant.WORK_RESPONSE_SYMBOL));
                report.setException((String)retMap.get(Constant.WORK_EXCEPTION));
                if(report.getException() != null) {
                	report.setError(true);
                }
                report.setMetadata(retMap);
                latch.countDown();
            } finally {
                ret.release();
                ctx.close();
            }
        }
    }

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public boolean isSubmit() {
		return isSubmit;
	}

	public boolean isAsync() {
		return async;
	}

	public void setAsync(boolean async) {
		this.async = async;
	}

    
    
}
