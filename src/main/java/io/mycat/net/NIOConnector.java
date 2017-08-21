/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import java.util.concurrent.atomic.AtomicLong;
/**
 * 作为客户端去连接后台数据库（MySql，后端NIO通信）
 * @author mycat
 */
public final class NIOConnector extends Thread implements SocketConnector {
	private static final Logger LOGGER = LoggerFactory.getLogger(NIOConnector.class);
	public static final ConnectIdGenerator ID_GENERATOR = new ConnectIdGenerator();

	private final String name;
	private final Selector selector;
	private final BlockingQueue<AbstractConnection> connectQueue;
	private long connectCount;
	/**
	 * 一般高性能网络通信框架采用多Reactor（多dispatcher）模式，这里将NIOReactor池化；
	 * 每次NIOConnector接受一个连接或者NIOAcceptor请求一个连接，都会封装成AbstractConnection，
	 * 同时请求NIOReactorPool每次轮询出一个NIOReactor，之后AbstractConnection与这个NIOReactor绑定
	 */
	private final NIOReactorPool reactorPool;

	public NIOConnector(String name, NIOReactorPool reactorPool)
			throws IOException {
		super.setName(name);
		this.name = name;
		this.selector = Selector.open();
		this.reactorPool = reactorPool;
		// LinkedBlockingQueue实现是线程安全的，实现了先进先出等特性，是作为生产者消费者的首选
		this.connectQueue = new LinkedBlockingQueue<AbstractConnection>();
	}

	public long getConnectCount() {
		return connectCount;
	}

	public void postConnect(AbstractConnection c) {
		connectQueue.offer(c);
		selector.wakeup();
	}

	@Override
	public void run() {
		final Selector tSelector = this.selector;
		for (;;) {
			++connectCount;
			try {
				// 查看有无连接就绪 阻塞的
				tSelector.select(1000L);
				connect(tSelector);
				Set<SelectionKey> keys = tSelector.selectedKeys();
				try {
					for (SelectionKey key : keys) {
						Object att = key.attachment();
						if (att != null && key.isValid() && key.isConnectable()) {
							finishConnect(key, att);
						} else {
							key.cancel();
						}
					}
				} finally {
					keys.clear();
				}
			} catch (Exception e) {
				LOGGER.warn(name, e);
			}
		}
	}

	private void connect(Selector selector) {
		AbstractConnection c = null;
		while ((c = connectQueue.poll()) != null) {
			try {
				SocketChannel channel = (SocketChannel) c.getChannel();
				// 注册 OP_CONNECT(建立连接) 监听与后端连接是否真正建立  // 监听到之后是图-MySql第3步，(TCP连接建立)
				channel.register(selector, SelectionKey.OP_CONNECT, c);
				// 主动连接  阻塞或者非阻塞  // 图-MySql第1步，(TCP连接请求)
				channel.connect(new InetSocketAddress(c.host, c.port));

			} catch (Exception e) {
				LOGGER.error("error:",e);
				c.close(e.toString());
			}
		}
	}

	private void finishConnect(SelectionKey key, Object att) {
		BackendAIOConnection c = (BackendAIOConnection) att;
		try {
			// 做原生NIO连接是否完成的判断和操作
			if (finishConnect(c, (SocketChannel) c.channel)) {
				clearSelectionKey(key);
				c.setId(ID_GENERATOR.getId());
				// 绑定特定的NIOProcessor以作idle清理
				NIOProcessor processor = MycatServer.getInstance()
						.nextProcessor();
				c.setProcessor(processor);
				// 与特定NIOReactor绑定监听读写
				NIOReactor reactor = reactorPool.getNextReactor();
				reactor.postRegister(c);
				// 连接后台真正完成
				c.onConnectfinish();
			}
		} catch (Exception e) {
			// 如有异常，将key清空
			clearSelectionKey(key);
			LOGGER.error("error:",e);
            c.close(e.toString());
			c.onConnectFailed(e);

		}
	}

	private boolean finishConnect(AbstractConnection c, SocketChannel channel)
			throws IOException {
		if (channel.isConnectionPending()) {
			// 阻塞或非阻塞
			channel.finishConnect();

			c.setLocalPort(channel.socket().getLocalPort());
			return true;
		} else {
			return false;
		}
	}

	private void clearSelectionKey(SelectionKey key) {
		if (key.isValid()) {
			key.attach(null);
			key.cancel();
		}
	}

	/**
	 * 后端连接ID生成器
	 * @author mycat
	 */
	public static class ConnectIdGenerator {

		private static final long MAX_VALUE = Long.MAX_VALUE;

		private long connectId = 0L;
		private final Object lock = new Object();

		public long getId() {
			synchronized (lock) {
				if (connectId >= MAX_VALUE) {
					connectId = 0L;
				}
				return ++connectId;
			}
		}
	}

}
