package com.lobstar.base.role.master;

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

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.client.Client;

import com.lobstar.config.Constant;
import com.lobstar.index.QueryTools;
import com.lobstar.manage.IDistributionHandlerInNetty;
import com.lobstar.utils.Utils;

public class MissionWindow {

	private Master master;
	
	private ServerBootstrap bootstrap;
	private ObjectMapper objectMapper = new ObjectMapper();
	private String host;
	private int port = 0;

	private Client client;

	private CopyOnWriteArrayList<String> workerSet = ServantGroup.getWorkerSet();

	private Map<String, CopyOnWriteArrayList<String>> domainWorkerMap = ServantGroup
			.getDomainWorkerMap();

	private IDistributionHandlerInNetty distributionHandlerInNetty = new IDistributionHandlerInNetty() {

		@Override
		public String distribution(Map<String, Object> source,
				CopyOnWriteArrayList<String> workerSet,
				Map<String, CopyOnWriteArrayList<String>> domainWorkerMap) {
			if (source != null) {
				Object domain = source.get(Constant.WORK_DOMAIN_SYMBOL);
				if (domain != null) {
					if (domainWorkerMap.containsKey(domain)
							&& domainWorkerMap.get(domain).size() > 0) {
						int label = Math
								.abs(source.hashCode() % domainWorkerMap.get(domain).size());
						String type = domainWorkerMap.get(domain).get(label);
						return type;
					}
				}
			}
			int label = Math.abs(source.hashCode() % workerSet.size());
			String type = workerSet.get(label);
			return type;
		}
	};

	public MissionWindow() {
		EventLoopGroup parentGroup = new NioEventLoopGroup(
				Constant.MASTER_TICKET_WINDOW_THREAD_SIZE);
		EventLoopGroup childGroup = new NioEventLoopGroup(Constant.MASTER_TICKET_WINDOW_THREAD_SIZE);
		this.bootstrap = new ServerBootstrap();
		ServerHandler serverHandler = new ServerHandler();
		bootstrap.group(parentGroup, childGroup).channel(NioServerSocketChannel.class)
				.childHandler(serverHandler).option(ChannelOption.SO_KEEPALIVE, true);
	}

	public MissionWindow(Master master,String host, int port) {
		this();
		this.host = host;
		this.port = port;
		this.master = master;
	}

	public void open(Client client) throws InterruptedException {
		this.client = client;
		if (this.host == null) {
			bootstrap.bind(this.port).sync();
		} else {
			if (this.port == 0) {
				this.port = Constant.TICKET_PORT;
			}
			bootstrap.bind(this.host, this.port).sync();
		}

	}

	private class ServerHandler extends ChannelInitializer<SocketChannel> {

		@Override
		protected void initChannel(SocketChannel ch) throws Exception {
			ch.pipeline().addLast("encoder", new LengthFieldPrepender(4, false));
			ch.pipeline().addLast(new InBoundChannelHandler());
		}
	}

	private class InBoundChannelHandler extends ChannelInboundHandlerAdapter {
		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
			ByteBuf byteBuf = (ByteBuf) msg;
			try {
				Map<String, Object> data = fetchTaskData(byteBuf);
				data.put(Constant.VISITOR_TIME_SYMBOL, Utils.getSystemTime());
				data.put(Constant.WORK_ASSIGN_SYMBOL, "true");
		        data.put(Constant.WORK_DONE_SYMBOL, "false");
				String type = null;
				Object broadcast = data.get(Constant.WORK_DOMAIN_BROADCAST);
				byte[] response = null;
				if (broadcast != null && broadcast.equals("true")) {
					String id = master.getBroadcastQueue().add(data);
					
					String string = master.getBroadcastQueue().response(id);
					Map<String,Object> ret = new HashMap<String, Object>();
					ret.put(Constant.WORK_RESPONSE_SYMBOL, string);
					response = objectMapper.writeValueAsString(ret).getBytes();
				}else {
					synchronized (MissionWindow.this) {
						type = distributionHandlerInNetty
								.distribution(data, workerSet, domainWorkerMap);
					}
					if (type != null) {
						String index = QueryTools.getDailyIndex();
						String id = addTask(index, type, data);
						response = fetchTaskResponse(index, id);					
					}
				}
				if (response != null) {
					ByteBuf retBuf = ctx.alloc().buffer(response.length);
					retBuf.writeBytes(response);
					ctx.channel().writeAndFlush(retBuf);
				}
				
			} finally {
				byteBuf.release();
			}
		}
	}

	private Map<String, Object> fetchTaskData(ByteBuf byteBuf) throws Exception {
		int readableBytes = byteBuf.readableBytes();
		byte[] readBytes = new byte[readableBytes];
		byteBuf.readBytes(readBytes);
		String reciveMsg = new String(readBytes, Charset.forName(Constant.GLOBAL_CHARSET));
		@SuppressWarnings("unchecked")
		Map<String, Object> jsonObject = objectMapper.readValue(reciveMsg, Map.class);
		return jsonObject;
	}

	private String addTask(String index, String type, Map<String, Object> data) {
		String id = null;
		try {
			id = QueryTools.insertIndex(client, index, type, data);
		} catch (Exception e) {
			if (!QueryTools.isIndexExist(client, index)) {
				QueryTools.createIndex(client, index);
				id = QueryTools.insertIndex(client, index, type, data);
			}
		}
		return id;
	}

	private byte[] fetchTaskResponse(String index, String id) {
		for (int i = 0; i < 500; i++) {
			try {
				Map<String, Object> retSource = QueryTools.getIndexAndTypeById(client, index, id);
				if (retSource != null) {
					Object retObj = retSource.get(Constant.WORK_DONE_SYMBOL);
					retSource.put(Constant.WORK_RESPSONSE_ASYNC_TASK_ID, id);
					retSource.put(Constant.WORK_RESPSONSE_ASYNC_TASK_INDEX, index);
					if (!"false".equals(retObj)) {
						String jsonData = objectMapper.writeValueAsString(retSource);
						byte[] retBytes = jsonData.toString().getBytes(
								Charset.forName(Constant.GLOBAL_CHARSET));
						return retBytes;
					}
				}
				Thread.sleep(200);
			} catch (Exception e) {

			}
		}
		return null;
	}

}
