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
import java.nio.channels.AsynchronousChannel;
import java.nio.channels.NetworkChannel;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Strings;

import io.mycat.backend.mysql.CharsetUtil;
import io.mycat.util.CompressUtil;
import io.mycat.util.TimeUtil;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

/**
 * 就是NIO的channel的封装
 * @author mycat
 */
public abstract class AbstractConnection implements NIOConnection {
	
	protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractConnection.class);
	
	protected String host;
	protected int localPort;
	protected int port;
	protected long id;
	protected volatile String charset;
	protected volatile int charsetIndex;

	protected final NetworkChannel channel;
	// 每个AbstractConnection依赖于一个NIOProcessor，每个NIOProcessor保存着多个AbstractConnection。
	// NIOProcessor是对AbstractConnection实现NIO读写的方法类
	protected NIOProcessor processor;
	// NIOHandler是处理AbstractConnection读取的数据的处理方法类
	protected NIOHandler handler;

	protected int packetHeaderSize;
	protected int maxPacketSize;
	// AbstractConnection的方法只对它里面的buffer进行操作，
	// 而buffer与channel之间的交互，是通过NIOSocketWR的方法完成的
	protected volatile ByteBuffer readBuffer;
	protected volatile ByteBuffer writeBuffer;
	
	protected final ConcurrentLinkedQueue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<ByteBuffer>();
	
	protected volatile int readBufferOffset;
	protected long lastLargeMessageTime;
	protected final AtomicBoolean isClosed;
	protected boolean isSocketClosed;
	protected long startupTime;
	protected long lastReadTime;
	protected long lastWriteTime;
	protected long netInBytes;
	protected long netOutBytes;
	protected int writeAttempts;
	
	protected volatile boolean isSupportCompress = false;
    protected final ConcurrentLinkedQueue<byte[]> decompressUnfinishedDataQueue = new ConcurrentLinkedQueue<byte[]>();
    protected final ConcurrentLinkedQueue<byte[]> compressUnfinishedDataQueue = new ConcurrentLinkedQueue<byte[]>();

	private long idleTimeout;
	// NIOSocketWR是执行以上方法的线程类。
	private final SocketWR socketWR;

	// AbstractConnection其实就是把Java的NetworkChannel进行封装，
	// 同时需要依赖其他几个类来完成他所需要的操作
	public AbstractConnection(NetworkChannel channel) {
		this.channel = channel;
		boolean isAIO = (channel instanceof AsynchronousChannel);
		if (isAIO) {
			socketWR = new AIOSocketWR(this);
		} else {
			socketWR = new NIOSocketWR(this);
		}
		this.isClosed = new AtomicBoolean(false);
		this.startupTime = TimeUtil.currentTimeMillis();
		this.lastReadTime = startupTime;
		this.lastWriteTime = startupTime;
	}

	public String getCharset() {
		return charset;
	}

	public boolean setCharset(String charset) {

		// 修复PHP字符集设置错误, 如： set names 'utf8'
		if (charset != null) {
			charset = charset.replace("'", "");
		}

		int ci = CharsetUtil.getIndex(charset);
		if (ci > 0) {
			this.charset = charset.equalsIgnoreCase("utf8mb4") ? "utf8" : charset;
			this.charsetIndex = ci;
			return true;
		} else {
			return false;
		}
	}

	public boolean isSupportCompress() {
		return isSupportCompress;
	}

	public void setSupportCompress(boolean isSupportCompress) {
		this.isSupportCompress = isSupportCompress;
	}

	public int getCharsetIndex() {
		return charsetIndex;
	}

	public long getIdleTimeout() {
		return idleTimeout;
	}

	public SocketWR getSocketWR() {
		return socketWR;
	}

	public void setIdleTimeout(long idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	public int getLocalPort() {
		return localPort;
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

	public void setLocalPort(int localPort) {
		this.localPort = localPort;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public boolean isIdleTimeout() {
		return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime, lastReadTime) + idleTimeout;
	}

	public NetworkChannel getChannel() {
		return channel;
	}

	public int getPacketHeaderSize() {
		return packetHeaderSize;
	}

	public void setPacketHeaderSize(int packetHeaderSize) {
		this.packetHeaderSize = packetHeaderSize;
	}

	public int getMaxPacketSize() {
		return maxPacketSize;
	}

	public void setMaxPacketSize(int maxPacketSize) {
		this.maxPacketSize = maxPacketSize;
	}

	public long getStartupTime() {
		return startupTime;
	}

	public long getLastReadTime() {
		return lastReadTime;
	}

	public void setProcessor(NIOProcessor processor) {
		this.processor = processor;
		int size = processor.getBufferPool().getChunkSize();
		this.readBuffer = processor.getBufferPool().allocate(size);
	}

	public long getLastWriteTime() {
		return lastWriteTime;
	}

	public long getNetInBytes() {
		return netInBytes;
	}

	public long getNetOutBytes() {
		return netOutBytes;
	}

	public int getWriteAttempts() {
		return writeAttempts;
	}

	public NIOProcessor getProcessor() {
		return processor;
	}

	public ByteBuffer getReadBuffer() {
		return readBuffer;
	}

	public ByteBuffer allocate() {
		int size = this.processor.getBufferPool().getChunkSize();
		ByteBuffer buffer = this.processor.getBufferPool().allocate(size);
		return buffer;
	}

	public final void recycle(ByteBuffer buffer) {
		this.processor.getBufferPool().recycle(buffer);
	}

	public void setHandler(NIOHandler handler) {
		this.handler = handler;
	}

	@Override
	public void handle(byte[] data) {
		if (isSupportCompress()) {
			List<byte[]> packs = CompressUtil.decompressMysqlPacket(data, decompressUnfinishedDataQueue);
			for (byte[] pack : packs) {
				if (pack.length != 0) {
					handler.handle(pack);
				}
			}
		} else {
			handler.handle(data);
		}
	}

	@Override
	public void register() throws IOException {

	}

	public void asynRead() throws IOException {
		this.socketWR.asynRead();
	}

	public void doNextWriteCheck() throws IOException {
		this.socketWR.doNextWriteCheck();
	}

	/**
	 * 读取可能的Socket字节流
	 */
	public void onReadData(int got) throws IOException {
		// 如果连接已经关闭，则不处理
		if (isClosed.get()) {
			return;
		}
		
		lastReadTime = TimeUtil.currentTimeMillis();
		// 读取到的字节小于0，表示流关闭，如果等于0，代表TCP连接关闭了
		if (got < 0) {
			this.close("stream closed");
            return;
		} else if (got == 0
				&& !this.channel.isOpen()) {
				this.close("socket closed");
				return;
		}
		netInBytes += got;
		processor.addNetInBytes(got);

		// 循环处理字节信息
		// readBuffer一直处于write mode，position记录最后的写入位置
		int offset = readBufferOffset, length = 0, position = readBuffer.position();
		for (;;) {
			// 获取包头的包长度信息
			length = getPacketLength(readBuffer, offset);			
			if (length == -1) {
				if (offset != 0) {
					this.readBuffer = compactReadBuffer(readBuffer, offset);
				} else if (readBuffer != null && !readBuffer.hasRemaining()) {
					throw new RuntimeException( "invalid readbuffer capacity ,too little buffer size " 
							+ readBuffer.capacity());
				}
				break;
			}
			// 如果postion小于包起始位置加上包长度，证明readBuffer不够大，需要扩容
			if (position >= offset + length && readBuffer != null) {
				
				// handle this package
				readBuffer.position(offset);				
				byte[] data = new byte[length];
				// 读取一个完整的包
				readBuffer.get(data, 0, length);
				// 处理包，每种AbstractConnection的处理函数不同
				// 每种AbstractConnection的handler不同，FrontendConnection的handler为FrontendAuthenticator
				// FrontendConnection会接收什么请求呢？有两种，认证请求和SQL命令请求。
				// 只有认证成功后，才会接受SQL命令请求。
				// FrontendAuthenticator只负责认证请求，在认证成功后，
				// 将对应AbstractConnection的handler设为处理SQL请求的FrontendCommandHandler即可
				handle(data);
				
				// maybe handle stmt_close
				if(isClosed()) {
					return ;
				}

				// offset to next position
				// 记录下读取到哪里了
				offset += length;
				
				// reached end
				// 如果最后写入位置等于最后读取位置，则证明所有的处理完了，可以清空缓存和offset
				// 否则，记录下最新的offset
				// 由于readBufferOffset只会单线程（绑定的RW线程）修改，
				// 但是会有多个线程访问（定时线程池的清理任务），所以设为volatile，不用CAS，
				// 这是一个经典的用volatile代替CAS实现多线程安全访问的场景。

				if (position == offset) {
					// if cur buffer is temper none direct byte buffer and not
					// received large message in recent 30 seconds
					// then change to direct buffer for performance
					if (readBuffer != null && !readBuffer.isDirect()
							&& lastLargeMessageTime < lastReadTime - 30 * 1000L) {  // used temp heap
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("change to direct con read buffer ,cur temp buf size :" + readBuffer.capacity());
						}
						recycle(readBuffer);
						readBuffer = processor.getBufferPool().allocate(processor.getBufferPool().getConReadBuferChunk());
					} else {
						if (readBuffer != null) {
							readBuffer.clear();
						}
					}
					// no more data ,break
					readBufferOffset = 0;
					break;
				} else {
					// try next package parse
					readBufferOffset = offset;
					if(readBuffer != null) {
						readBuffer.position(position);
					}
					continue;
				}
				
				
				
			} else {				
				// not read whole message package ,so check if buffer enough and
				// compact readbuffer
				if (!readBuffer.hasRemaining()) {
					readBuffer = ensureFreeSpaceOfReadBuffer(readBuffer, offset, length);
				}
				break;
			}
		}
	}
	
	private boolean isConReadBuffer(ByteBuffer buffer) {
		return buffer.capacity() == processor.getBufferPool().getConReadBuferChunk() && buffer.isDirect();
	}
	
	private ByteBuffer ensureFreeSpaceOfReadBuffer(ByteBuffer buffer,
			int offset, final int pkgLength) {
		// need a large buffer to hold the package
		if (pkgLength > maxPacketSize) {
			throw new IllegalArgumentException("Packet size over the limit.");
		} else if (buffer.capacity() < pkgLength) {
		
			ByteBuffer newBuffer = processor.getBufferPool().allocate(pkgLength);
			lastLargeMessageTime = TimeUtil.currentTimeMillis();
			buffer.position(offset);
			newBuffer.put(buffer);
			readBuffer = newBuffer;

			recycle(buffer);
			readBufferOffset = 0;
			return newBuffer;

		} else {
			if (offset != 0) {
				// compact bytebuffer only
				return compactReadBuffer(buffer, offset);
			} else {
				throw new RuntimeException(" not enough space");
			}
		}
	}
	
	private ByteBuffer compactReadBuffer(ByteBuffer buffer, int offset) {
		if(buffer == null) {
			return null;
		}
		buffer.limit(buffer.position());
		buffer.position(offset);
		buffer = buffer.compact();
		readBufferOffset = 0;
		return buffer;
	}

	public void write(byte[] data) {
		ByteBuffer buffer = allocate();
		buffer = writeToBuffer(data, buffer);
		write(buffer);

	}

	private final void writeNotSend(ByteBuffer buffer) {
		if (isSupportCompress()) {
			ByteBuffer newBuffer = CompressUtil.compressMysqlPacket(buffer, this, compressUnfinishedDataQueue);
			writeQueue.offer(newBuffer);
			
		} else {
			writeQueue.offer(buffer);
		}
	}


    @Override
	public final void write(ByteBuffer buffer) {
		// 首先判断是否为压缩协议
		if (isSupportCompress()) {
			// CompressUtil为压缩协议辅助工具类
			ByteBuffer newBuffer = CompressUtil.compressMysqlPacket(buffer, this, compressUnfinishedDataQueue);
			// 将要写的数据先放入写缓存队列
			writeQueue.offer(newBuffer);
		} else {
			// 将要写的数据先放入写缓存队列
			writeQueue.offer(buffer);
		}

		// if ansyn write finishe event got lock before me ,then writing
		// flag is set false but not start a write request
		// so we check again
		try {
			// 处理写事件，这个方法比较复杂，需要重点分析其思路
			// 这里doNextWriteCheck() 方法调用是主要调用，所有往AbstractionConnection中的写入都会调用Abstraction.write(ByteBuffer)，
			// 这个方法先把要写的放入缓存队列
			this.socketWR.doNextWriteCheck();
		} catch (Exception e) {
			LOGGER.warn("write err:", e);
			this.close("write err:" + e);
		}
	}

	
	public ByteBuffer checkWriteBuffer(ByteBuffer buffer, int capacity, boolean writeSocketIfFull) {
		if (capacity > buffer.remaining()) {
			if (writeSocketIfFull) {
				writeNotSend(buffer);
				return processor.getBufferPool().allocate(capacity);
			} else {// Relocate a larger buffer
				buffer.flip();
				ByteBuffer newBuf = processor.getBufferPool().allocate(capacity + buffer.limit() + 1);
				newBuf.put(buffer);
				this.recycle(buffer);
				return newBuf;
			}
		} else {
			return buffer;
		}
	}

	public ByteBuffer writeToBuffer(byte[] src, ByteBuffer buffer) {
		int offset = 0;
		int length = src.length;
		int remaining = buffer.remaining();
		while (length > 0) {
			if (remaining >= length) {
				buffer.put(src, offset, length);
				break;
			} else {
				buffer.put(src, offset, remaining);
				writeNotSend(buffer);
				buffer = allocate();
				offset += remaining;
				length -= remaining;
				remaining = buffer.remaining();
				continue;
			}
		}
		return buffer;
	}

	@Override
	public void close(String reason) {
		if (!isClosed.get()) {
			closeSocket();
			isClosed.set(true);
			if (processor != null) {
				processor.removeConnection(this);
			}
			this.cleanup();
			isSupportCompress = false;

			// ignore null information
			if (Strings.isNullOrEmpty(reason)) {
				return;
			}
			LOGGER.info("close connection,reason:" + reason + " ," + this);
			if (reason.contains("connection,reason:java.net.ConnectException")) {
				throw new RuntimeException(" errr");
			}
		}
	}

	public boolean isClosed() {
		return isClosed.get();
	}

	public void idleCheck() {
		if (isIdleTimeout()) {
			LOGGER.info(toString() + " idle timeout");
			close(" idle ");
		}
	}

	/**
	 * 清理资源 清理连接占用的缓存资源
	 */
	protected void cleanup() {
		
		// 清理资源占用
		if (readBuffer != null) {
			// 回收读缓冲
			this.recycle(readBuffer);
			this.readBuffer = null;
			this.readBufferOffset = 0;
		}
		
		if (writeBuffer != null) {
			// 回收写缓冲
			recycle(writeBuffer);
			this.writeBuffer = null;
		}
		// 回收压缩协议栈编码解码队列
		if (!decompressUnfinishedDataQueue.isEmpty()) {
			decompressUnfinishedDataQueue.clear();
		}
		
		if (!compressUnfinishedDataQueue.isEmpty()) {
			compressUnfinishedDataQueue.clear();
		}
		// 回收写队列
		ByteBuffer buffer = null;
		while ((buffer = writeQueue.poll()) != null) {
			recycle(buffer);
		}
	}
	
	protected int getPacketLength(ByteBuffer buffer, int offset) {
		int headerSize = getPacketHeaderSize();
		if ( isSupportCompress() ) {
			headerSize = 7;
		}
		
		if (buffer.position() < offset + headerSize) {
			return -1;
		} else {
			int length = buffer.get(offset) & 0xff;
			length |= (buffer.get(++offset) & 0xff) << 8;
			length |= (buffer.get(++offset) & 0xff) << 16;
			return length + headerSize;
		}
	}

	public ConcurrentLinkedQueue<ByteBuffer> getWriteQueue() {
		return writeQueue;
	}

	private void closeSocket() {
		if (channel != null) {
			
			boolean isSocketClosed = true;
			try {
				channel.close();
			} catch (Exception e) {
				LOGGER.error("AbstractConnectionCloseError", e);
			}
			
			boolean closed = isSocketClosed && (!channel.isOpen());
			if (closed == false) {
				LOGGER.warn("close socket of connnection failed " + this);
			}
		}
	}
	public void onConnectfinish() {
		LOGGER.debug("连接后台真正完成");
	}	
}
