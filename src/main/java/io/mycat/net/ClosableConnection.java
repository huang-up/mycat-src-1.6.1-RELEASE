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

/**
 * 根据字面意思，一个可以关闭的连接需要实现关闭方法-_-，并且需要原因判断是否是正常关闭。
 * MySQL的通信都需要指定字符集。
 * 1.监听端口 2.服务端口（通信端口）
 * MyCat服务器建立ServerSocket时输入的端口为服务器在其上面监听客户的连接,
 * 当有客户连接时,再随机选择一个没用的端口与客户端通信;
 * 建立客户socket时输入的为服务端的监听端口,在本地选择一个未用端口与服务器通信,
 * 至于服务器怎么知道和客户端的哪个端口通信,和客户端怎么知道和服务端的哪个端口通信(因为这两个端口都是随机生成的),
 * tcp是采用”三次握手”建立连接,而udp则是每次发送信息时将端口号放在ip报文的数据段里面。
 * 所以，连接里面需要提供获得监听端口和服务端口的方法。
 * 此外，还需要检查连接是否为空闲状态（idle）。
 * 最后，需要一些统计数据。
 */
public interface ClosableConnection {
	String getCharset();
	/**
	 * 关闭连接
	 */
	void close(String reason);

	boolean isClosed();

	void idleCheck();

	long getStartupTime();

	String getHost();

	int getPort();

	int getLocalPort();

	long getNetInBytes();

	long getNetOutBytes();
}