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
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

/**
 * 网络事件反应器
 * Reactor模式的NIO，处理并转发请求到RW线程，
 * 其实就是把对应AbstractConnection（就是NIO的channel的封装）,
 * 注册到RW线程的selector上，只注册读标记；
 * @author mycat, Uncle-pan
 * 
 */
public final class NIOReactor {
	private static final Logger LOGGER = LoggerFactory.getLogger(NIOReactor.class);
	private final String name;
	private final RW reactorR;

	public NIOReactor(String name) throws IOException {
		this.name = name;
		this.reactorR = new RW();
	}

	final void startup() {
		new Thread(reactorR, name + "-RW").start();
	}

	/**
	 * Reactor接收到的请求有两种，前端的SQL请求，还有后端的结果
	 * 但是这两种都不能直接转发，前端的SQL请求需要经过SQL解析等业务步骤才能写到后端，
	 * 后端的结果也需要经过业务处理才能写到前端。
	 * 所以只要执行业务步骤的线程去注册写标记，Reactor只要在检查到写标记后，去写之后，取消标记即可
	 * @param c 前端AbstractConnection还有后端AbstractConnection
	 */
	final void postRegister(AbstractConnection c) {
		/**
		 * 为什么放入RW线程的注册队列，而不是直接注册呢？
		 * 如果是直接注册，那么就是NIOAcceptor这个线程负责注册，这里就会有锁竞争，
		 * 因为NIOAcceptor这个线程和每个RW线程会去竞争selector的锁。这样NIOAcceptor就不能高效的处理连接。
		 * 所以，更好的方式是将FrontendConnection放入RW线程的注册队列，之后让RW线程自己完成注册工作。
		 */
		// RW线程的注册队列
		reactorR.registerQueue.offer(c);
		// 唤醒RW线程的selector
		reactorR.selector.wakeup();
	}

	final Queue<AbstractConnection> getRegisterQueue() {
		return reactorR.registerQueue;
	}

	final long getReactCount() {
		return reactorR.reactCount;
	}

	/**
	 * RW线程，负责执行NIO的channel读写，
	 * 这里channel封装成了AbstractConnection
	 */
	private final class RW implements Runnable {
		private final Selector selector;
		private final ConcurrentLinkedQueue<AbstractConnection> registerQueue;
		private long reactCount;

		private RW() throws IOException {
			this.selector = Selector.open();
			this.registerQueue = new ConcurrentLinkedQueue<AbstractConnection>();
		}

		@Override
		public void run() {
			final Selector selector = this.selector;
			Set<SelectionKey> keys = null;
			for (;;) {
				++reactCount;
				try {
					selector.select(500L);
					// 从注册队列中取出AbstractConnection
					// 注册读事件
					register(selector);
					keys = selector.selectedKeys();
					for (SelectionKey key : keys) {
						AbstractConnection con = null;
						try {
							Object att = key.attachment();
							if (att != null) {
								con = (AbstractConnection) att;
								// 监听到有效读
								if (key.isValid() && key.isReadable()) {
									try {
										// 异步读取数据并处理数据
										// heartbeat  select user() 走 MySQLConnectionHandler??
 										con.asynRead();
									} catch (IOException e) {
                                        con.close("program err:" + e.toString());
										continue;
									} catch (Exception e) {
										LOGGER.warn("caught err:", e);
										con.close("program err:" + e.toString());
										continue;
									}
								}
								if (key.isValid() && key.isWritable()) {
									//异步写数据??
									con.doNextWriteCheck();
								}
							} else {
								key.cancel();
							}
                        } catch (CancelledKeyException e) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(con + " socket key canceled");
                            }
                        } catch (Exception e) {
                            LOGGER.warn(con + " " + e);
                        } catch (final Throwable e){
                        	// Catch exceptions such as OOM and close connection if exists
                        	//so that the reactor can keep running!
                        	// @author Uncle-pan
                        	// @since 2016-03-30
                        	if(con != null){
                        		con.close("Bad: "+e);
                        	}
                        	LOGGER.error("caught err: ", e);
                        	continue;
                        }
					}
				} catch (Exception e) {
					LOGGER.warn(name, e);
				} catch (final Throwable e){
					// Catch exceptions such as OOM so that the reactor can keep running!
                	// @author Uncle-pan
                	// @since 2016-03-30
					LOGGER.error("caught err: ", e);
				} finally {
					if (keys != null) {
						keys.clear();
					}

				}
			}
		}

		private void register(Selector selector) {
			AbstractConnection c = null;
			if (registerQueue.isEmpty()) {
				return;
			}
			while ((c = registerQueue.poll()) != null) {
				try {
					// 注册读事件
					((NIOSocketWR) c.getSocketWR()).register(selector);
					// 连接注册，对于FrontendConnection是发送HandshakePacket并异步读取响应
					// 响应为AuthPacket，读取其中的信息，验证用户名密码等信息，如果符合条件
					// 则发送OkPacket
					c.register();
				} catch (Exception e) {
					c.close("register err" + e.toString());
					LOGGER.error("register err", e);
				}
			}
		}

	}

}
