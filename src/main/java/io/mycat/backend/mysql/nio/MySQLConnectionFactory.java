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
package io.mycat.backend.mysql.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.NetworkChannel;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.nio.handler.ResponseHandler;
import io.mycat.config.model.DBHostConfig;
import io.mycat.net.NIOConnector;
import io.mycat.net.factory.BackendConnectionFactory;

/**
 * 后端连接工厂
 * 那么这些后端连接是什么时候建立的呢？
 	首先，MyCat配置文件中，DataHost标签中有minIdle这个属性。
 	代表在MyCat初始化时，会在这个DataHost上初始化维护多少个连接（这些连接可以理解为连接池）。
 	每个前端Client连接会创建Session，而Session会根据命令的不同而创建不同的Handler。
 	每个Handler会从连接池中拿出所需要的连接并使用。
	在连接池大小不够时，RW线程会异步驱使新建所需的连接补充连接池，但是连接数最大不能超过配置的maxCon。
 	同时，如之前所述，有定时线程检查并回收空闲后端连接。但池中最小不会小于minCon。
 * @author mycat
 */
public class MySQLConnectionFactory extends BackendConnectionFactory {
	@SuppressWarnings({ "unchecked", "rawtypes" })
	// 这里传入的ResponseHandler为DelegateResponseHandler，在连接建立验证之后，会调用
	public MySQLConnection make(MySQLDataSource pool, ResponseHandler handler,
			String schema) throws IOException {
		// DBHost配置
		DBHostConfig dsc = pool.getConfig();
		// 根据是否为NIO返回SocketChannel或者AIO的AsynchronousSocketChannel
		NetworkChannel channel = openSocketChannel(MycatServer.getInstance()
				.isAIO());
		// 新建MySQLConnection
		MySQLConnection c = new MySQLConnection(channel, pool.isReadNode());
		// 根据配置初始化MySQLConnection
		MycatServer.getInstance().getConfig().setSocketParams(c, false);
		c.setHost(dsc.getIp());
		c.setPort(dsc.getPort());
		c.setUser(dsc.getUser());
		c.setPassword(dsc.getPassword());
		c.setSchema(schema);
		// 目前实际连接还未建立，handler为MySQL连接认证MySQLConnectionAuthenticatorHandler
		c.setHandler(new MySQLConnectionAuthenticatorHandler(c, handler));
		c.setPool(pool);
		c.setIdleTimeout(pool.getConfig().getIdleTimeout());
		// AIO和NIO连接方式建立实际的MySQL连接
		if (channel instanceof AsynchronousSocketChannel) {
			((AsynchronousSocketChannel) channel).connect(
					new InetSocketAddress(dsc.getIp(), dsc.getPort()), c,
					(CompletionHandler) MycatServer.getInstance()
							.getConnector());
		} else {
			// 通过NIOConnector建立连接
			// 通过NIOConnector建立实际连接的过程与前端连接的建立相似，
			// 也是先放在队列中，之后由NIOConnector去建立连接
			((NIOConnector) MycatServer.getInstance().getConnector())
					.postConnect(c);

		}
		return c;
	}

}