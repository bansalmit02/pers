/**
 * 
 */
package com.intrade.etrading.wsp.common;

import static com.google.common.base.Preconditions.checkArgument;
import static com.intrade.commons.util.datetime.DateTimeUtils.nowInNanos;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.IoUtils;

import com.intrade.commons.util.annotation.concurrent.SingleThreadedUse;
import com.intrade.commons.util.datetime.DateTimeUtils;
import com.intrade.commons.util.functor.ObjectAndTwoLongsConsumer;
import com.intrade.commons.util.pool.ArrayDequeObjectPool;
import com.intrade.commons.util.pool.ObjectPool;
import com.intrade.commons.util.values.ByteBufferValue;
import com.intrade.etrading.undertow.registry.StreamSubscriptionRegistry;
import com.intrade.etrading.undertow.registry.StreamSubscriptionRegistry.MyWebSocketChannel;

import io.undertow.websockets.core.WebSocketCallback;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * 
 * @author Venny Sahukara
 *
 */
@SingleThreadedUse
public class UIConnectionFacade implements UIMessageDistributor<WebSocketChannel, ByteBufferValue> {
    private static final Logger LOGGER = LoggerFactory.getLogger(UIConnectionFacade.class);
    private final StreamSubscriptionRegistry registry;
    private final ObjectAndTwoLongsConsumer<ByteBufferValue> payloadLogger;
    private final TickWorkflowServicePlusPlus tickWorkflowService;
    
    private final ObjectPool<ByteBuffer> byteBufferPool;
    
    private UIConnectionFacade(Builder builder) {
        this.registry = builder.registry;
        this.payloadLogger = builder.payloadLogger;
        this.byteBufferPool = new ArrayDequeObjectPool<ByteBuffer>(builder.bufferPoolSize, ByteBuffer.class, () -> ByteBuffer.allocate(builder.maxBufferSize));
        this.tickWorkflowService = builder.tickWorkflowService;
    }
    
    @Override
    public boolean multicast(String streamId, ByteBufferValue payload) {
        boolean result = false;
        if(registry.hasSubscribers(streamId)){
            Collection<MyWebSocketChannel> subscribers = registry.getSubscribedChannelsForStream(streamId);
            //low-GC loop because of the reused Argona collection's iterator
            for(var channel : subscribers) {
                var didSend = unicast(channel.channel(), payload);
                result = didSend || result;
            }
        }
        
        return result;
    }
    
    @Override
    public boolean unicast(WebSocketChannel channel, ByteBufferValue payload) {
        boolean result;
        if(channel.isOpen() && !channel.isCloseInitiatedByRemotePeer()
                && !channel.isCloseFrameReceived()
                && !channel.isCloseFrameSent()) {
            var bbPayload = payload.value().rewind();
            var wspPublishTimestampNanos = nowInNanos();
            var clone = copy(bbPayload);
            WebSockets.sendText(clone, channel, myWebSocketCallback, clone);
            payloadLogger.accept(payload, registry.getChannelId(channel), wspPublishTimestampNanos);
            LOGGER.debug("json bytes sent on channel successfully");
            result = true;
        } else {
            LOGGER.debug("Closed channel in registry. It's expected to be removed subsequently!");
            result =  false;
        }
        return result;
    }
    
    @Override
    public void onWriteActionNotifiction(WebSocketChannel channel, ByteBuffer writeBuffer, Throwable error) {
        release(writeBuffer);
        LOGGER.debug("Write Buffer Released Back to pool on write action complete. WriteBufferPool stats [numActive={}, numIdle={}]", 
                byteBufferPool.getNumActive(), byteBufferPool.getNumIdle());
    }
    
    private ByteBuffer copy(ByteBuffer original) {
        original.rewind();
        var clone = acquire();
        clone.clear();
        clone.put(original);
        original.rewind();
        clone.flip();
        return clone;
    }
    
    private ByteBuffer acquire() {
        return byteBufferPool.acquire();
    }
    
    private void release(ByteBuffer buffer) {
        byteBufferPool.release(buffer);
    }
    
    private final MyWebSocketCallback myWebSocketCallback = new MyWebSocketCallback();
    
    @Setter
    @Accessors(fluent=true, chain=true)
    private class MyWebSocketCallback implements WebSocketCallback<ByteBuffer> {
        
        @Override
        public void complete(WebSocketChannel channel, ByteBuffer tempBuffer) {
            tickWorkflowService.publishWriteActionSuccessEvent(channel, tempBuffer, DateTimeUtils.nowInNanos());
        }
        
        @Override
        public void onError(WebSocketChannel channel, ByteBuffer tempBuffer, Throwable throwable) {
            if(!channel.isOpen() & throwable instanceof IOException) {
                LOGGER.debug("Attempt to send packets to a closed Channel. Nothing to worry as the channel registry will be eventually cleared upon an eventual Disonnect event. So this warning will eventually stop. Error: {}", throwable.getMessage());
            } else {
                LOGGER.error("Error on channel: ", throwable);
            }
            IoUtils.safeClose(channel);
            tickWorkflowService.publishWriteActionErrorEvent(channel, tempBuffer, throwable, DateTimeUtils.nowInNanos());
        }    
    }
    
    @Setter
    @Accessors(fluent = true, chain = true)
    public static class Builder {
        private StreamSubscriptionRegistry registry;
        private ObjectAndTwoLongsConsumer<ByteBufferValue> payloadLogger;
        private TickWorkflowServicePlusPlus tickWorkflowService;
        private int bufferPoolSize;
        private int maxBufferSize;
        
        public UIConnectionFacade build() {
            requireNonNull(registry);
            requireNonNull(payloadLogger);
            requireNonNull(tickWorkflowService);
            checkArgument(maxBufferSize > 0, "valid maxBufferSize must be specified");
            checkArgument(bufferPoolSize > 0, "valid bufferPoolSize must be specified");
            
            return new UIConnectionFacade(this);
        }
    }
}
