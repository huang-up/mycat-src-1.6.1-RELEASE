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
import java.nio.ByteBuffer;

/**
 * @author mycat
 */

/**
 * 所有NIO的通信需要在多路复用选择器上注册channel，这里有个对应的register()方法需要实现。
 * 然后，读取和写入数据都需要通过缓冲。
 * 缓冲区(Buffer)就是在内存中预留指定大小的存储空间用来对输入/输出(I/O)的数据作临时存储，
 * 这部分预留的内存空间就叫做缓冲区，使用缓冲区有这么两个好处：
    1. 减少实际的物理读写次数
    2. 缓冲区在创建时就被分配内存，这块内存区域一直被重用，可以减少动态分配和回收内存的次数
 */
public interface NIOConnection extends ClosableConnection{

    /**
     * connected 
     */
    void register() throws IOException;

    /**
     * 处理数据
     */
    void handle(byte[] data);

    /**
     * 写出一块缓存数据
     */
    void write(ByteBuffer buffer);
    
     
     
}