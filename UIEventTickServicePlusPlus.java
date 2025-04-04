package com.intrade.etrading.wsp.common;

import static com.google.common.base.Preconditions.checkArgument;
import static com.intrade.commons.util.concurrent.ExecutorServices.newScheduledDaemonSequentialExecutor;
import static com.intrade.commons.util.datetime.DateTimeUtils.uniqueNowInNanos;
import static com.intrade.commons.util.functor.RunnableWithException.rWrap;
import static java.util.Objects.requireNonNull;

import java.nio.ByteBuffer;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.intrade.commons.util.annotation.GCFree;
import com.intrade.commons.util.datetime.DateTimeUtils;
import com.intrade.commons.util.pool.ObjectPool;
import com.intrade.commons.util.values.BytesValue;
import com.intrade.etrading.undertow.handler.StreamCommand;
import com.intrade.etrading.undertow.registry.StreamSubscriptionRegistry;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;

import io.undertow.websockets.core.WebSocketChannel;
import lombok.Setter;
import lombok.experimental.Accessors;
import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.Bytes;

public class UIEventTickServicePlusPlus<E> implements TickWorkflowServicePlusPlus {
    private static final Logger LOGGER = LoggerFactory.getLogger(UIEventTickServicePlusPlus.class);
    private final Disruptor<UITickWorkflowEvent> uiTickEventDisruptor;
    private final UIEventNotifierPlusPlus<E> notifier;
    private final ObjectPool<E> tickEventPool;
    private final BiConsumer<E, Bytes<?>> tickEventUnMarshaller;
    private final Consumer<E> tickEventEnricher;
    private final boolean isTicksBatchingEnabled;
       
    private final ScheduledExecutorService tickTimer;
    private final long tickInterval;
    private final TimeUnit tickIntervalUnit;
    private volatile ScheduledFuture<?> tickTimerSchedule;
    
    private final StreamSubscriptionRegistry streamSubscriptionRegistry;
    
    private final boolean isCorePinningEnabled;
    private final int pinToCoreCpuId;

    private UIEventTickServicePlusPlus(Builder<E> builder) {
        this.uiTickEventDisruptor = builder.uiTickEventDisruptor;
        this.notifier = builder.uiEventNotifier;
        this.tickEventPool = builder.tickEventPool;
        this.tickEventUnMarshaller = builder.tickEventUnMarshaller;
        this.tickEventEnricher = builder.tickEventEnricher;
        this.isTicksBatchingEnabled = builder.isTicksBatchingEnabled;
        this.tickTimer = newScheduledDaemonSequentialExecutor("Tick-"+notifier.getName()+"-");
        this.tickInterval = builder.tickInterval;
        this.tickIntervalUnit = builder.tickIntervalUnit;
        this.streamSubscriptionRegistry = builder.streamSubscriptionRegistry;
        this.isCorePinningEnabled = builder.isCorePinningEnabled;
        this.pinToCoreCpuId = builder.pinToCoreCPUId;
        
    }
    
    @Override
    public String getName() {
        return notifier.getName();
    }
    
    @Override
    public void start() {
        notifier.start();
        uiTickEventDisruptor.handleEventsWith(new MyTickEventHandler());
        uiTickEventDisruptor.start();
        tickTimerSchedule = tickTimer.scheduleWithFixedDelay(this::publishTickTimerEvent, tickInterval, tickInterval, tickIntervalUnit);
    }
    
    @Override
    public void stop() {
        rWrap(() -> uiTickEventDisruptor.shutdown(5, TimeUnit.SECONDS)).run();
        notifier.stop();
        var schedule = tickTimerSchedule;
        if(schedule != null) schedule.cancel(true);
        tickTimer.shutdownNow();
    }
    
    @Override
    public boolean isAvailable() {
        return notifier.isAvailable();
    }
    
    @Override
    public void publishNewTickEvent(BytesValue uiNewTickEventBytes, long tickReceiveTimeNanos) {
        var ringBuffer = uiTickEventDisruptor.getRingBuffer();
        long sequence = ringBuffer.next();
        var nextSlotOnRingBuffer = ringBuffer.get(sequence);
        nextSlotOnRingBuffer.reset();
        nextSlotOnRingBuffer.tickWorkflowEventReceiveTimeNanos(tickReceiveTimeNanos);
        nextSlotOnRingBuffer.uiTickWorkflowEventType(UITickWorkflowEventType.NEW_TICK_EVENT);
        var uiNewTickEvent = nextSlotOnRingBuffer.uiNewTickEvent();
        uiNewTickEvent.copyEventBytes(uiNewTickEventBytes);
        ringBuffer.publish(sequence);
    }
    
    @Override
    public void publishTickTimerEvent() {
        var ringBuffer = uiTickEventDisruptor.getRingBuffer();
        long sequence = ringBuffer.next();
        var nextSlotOnRingBuffer = ringBuffer.get(sequence);
        nextSlotOnRingBuffer.reset();
        nextSlotOnRingBuffer.tickWorkflowEventReceiveTimeNanos(uniqueNowInNanos());
        nextSlotOnRingBuffer.uiTickWorkflowEventType(UITickWorkflowEventType.TICK_TIMER_EVENT);
        ringBuffer.publish(sequence);
    }
    
    //NOT my style - more than 3 method parameters!
    @Override
    public void publishTickStreamSubscriptionCommand(
            WebSocketChannel transportChannel,
            StreamCommand streamCommand,
            String streamId,
            long requestTimestampNanos) {
        
        var ringBuffer = uiTickEventDisruptor.getRingBuffer();
        long sequence = ringBuffer.next();
        var nextSlotOnRingBuffer = ringBuffer.get(sequence);
        nextSlotOnRingBuffer.reset();
        nextSlotOnRingBuffer.uiTickStreamSubscriptionEvent(transportChannel,
                streamCommand, streamId, requestTimestampNanos);
        nextSlotOnRingBuffer.uiTickWorkflowEventType(streamCommand);
        ringBuffer.publish(sequence);
    }
    
    @Override
    public void publishWriteActionSuccessEvent(WebSocketChannel transportChannel, ByteBuffer tempBufferToReturn,
            long actionNoitificationTimestampNanos) {
        var ringBuffer = uiTickEventDisruptor.getRingBuffer();
        long sequence = ringBuffer.next();
        var nextSlotOnRingBuffer = ringBuffer.get(sequence);
        nextSlotOnRingBuffer.reset();
        nextSlotOnRingBuffer.uiTickStreamChannelWriteActionNotificationEvent(transportChannel, tempBufferToReturn, actionNoitificationTimestampNanos);
        nextSlotOnRingBuffer.uiTickWorkflowEventType(UITickWorkflowEventType.TICK_STREAM_SUBSCRIPTION_CHANNEL_WRITE_SUCCESS_EVENT);
        ringBuffer.publish(sequence);
    }
    
    @Override
    public void publishWriteActionErrorEvent(WebSocketChannel transportChannel, ByteBuffer tempBufferToReturn,
            Throwable writeError, long actionNotificationTimestampNanos) {
        var ringBuffer = uiTickEventDisruptor.getRingBuffer();
        long sequence = ringBuffer.next();
        var nextSlotOnRingBuffer = ringBuffer.get(sequence);
        nextSlotOnRingBuffer.reset();
        nextSlotOnRingBuffer.uiTickStreamChannelWriteActionNotificationEvent(transportChannel, tempBufferToReturn, writeError, actionNotificationTimestampNanos);
        nextSlotOnRingBuffer.uiTickWorkflowEventType(UITickWorkflowEventType.TICK_STREAM_SUBSCRIPTION_CHANNEL_WRITE_ERROR_EVENT);
        ringBuffer.publish(sequence);
    }
    
    @Override
    public void publishBcastAllToAllUsersOnSTOMPEvent() {
        var ringBuffer = uiTickEventDisruptor.getRingBuffer();
        long sequence = ringBuffer.next();
        var nextSlotOnRingBuffer = ringBuffer.get(sequence);
        nextSlotOnRingBuffer.reset();
        nextSlotOnRingBuffer.tickWorkflowEventReceiveTimeNanos(uniqueNowInNanos());
        nextSlotOnRingBuffer.uiTickWorkflowEventType(UITickWorkflowEventType.BCAST_ALL_TO_ALL_USERS_ON_STOMP);
        ringBuffer.publish(sequence);
    }

    @Override
    public void publishBcastAllToGivenUserOnSTOMPEvent(String userName) {
        var ringBuffer = uiTickEventDisruptor.getRingBuffer();
        long sequence = ringBuffer.next();
        var nextSlotOnRingBuffer = ringBuffer.get(sequence);
        nextSlotOnRingBuffer.reset();
        nextSlotOnRingBuffer.tickWorkflowEventReceiveTimeNanos(uniqueNowInNanos());
        nextSlotOnRingBuffer.uiTickWorkflowEventType(UITickWorkflowEventType.BCAST_ALL_TO_GIVEN_USER_ON_STOMP);
        nextSlotOnRingBuffer.userName(userName);
        ringBuffer.publish(sequence);
    }

    public boolean isBatchingEnabled() {
        return isTicksBatchingEnabled;
    }
    
    public boolean isBatchingDisabled() {
        return !isBatchingEnabled();
    }
    
    private class MyTickEventHandler implements EventHandler<UITickWorkflowEvent> {
        private AffinityLock affinityLock;
        private boolean isFirstEvent = true;

        @Override
        public void onEvent(UITickWorkflowEvent event, long sequence, boolean endOfBatch) throws Exception {
            try {
                handleEvent(event, sequence, endOfBatch);
            } catch(Exception e) {
                LOGGER.error("Exception Handling Event: ", e);
            }
        }
        
        private void handleEvent(UITickWorkflowEvent event, long sequence, boolean endOfBatch) {
            if (isFirstEvent) {
                doCorePinningIfEnabled();
            }
            
            switch(event.tickWorkflowEventType()) {
                case NEW_TICK_EVENT -> onNewTickEvent(event, sequence, endOfBatch);
                case TICK_TIMER_EVENT -> onTickTimerEvent(event, sequence, endOfBatch);
                case ADD_TICK_STREAM_SUBSCRIPTION_EVENT -> onAddTickStreamSubscriptionEvent(event, sequence, endOfBatch);
                case REMOVE_TICK_STREAM_SUBSCRIPTION_EVENT -> onRemoveTickStreamSubscriptionEvent(event, sequence, endOfBatch);
                case TICK_STREAM_SUBSCRIPTION_CHANNEL_DISCONNECT_EVENT -> onDisconnectTickStreamSubscriptionChannelEvent(event, sequence, endOfBatch);
                case TICK_STREAM_SUBSCRIPTION_CHANNEL_WRITE_SUCCESS_EVENT -> onTickStreamChannelWriteSuccessEvent(event, sequence, endOfBatch);
                case TICK_STREAM_SUBSCRIPTION_CHANNEL_WRITE_ERROR_EVENT -> onTickStreamChannelWriteErrorEvent(event, sequence, endOfBatch);
                case BCAST_ALL_TO_ALL_USERS_ON_STOMP -> onMulticastAllToAllUsersOnSTOMPEvent(event, sequence, endOfBatch);
                case BCAST_ALL_TO_GIVEN_USER_ON_STOMP -> onMulticastAllToGivenUserOnSTOMPEvent(event, sequence, endOfBatch);
                case UNDEFINED_EVENT -> LOGGER.error("Undefined tick event type!!!");
            }
            
            if(isFirstEvent) {
                isFirstEvent = false; 
            }
        }
        
        private void doCorePinningIfEnabled() {
            if(isCorePinningEnabled) {
                if(affinityLock == null && pinToCoreCpuId > 0) {
                    affinityLock = AffinityLock.acquireLock(pinToCoreCpuId);
                } else if(affinityLock == null) {
                    affinityLock = AffinityLock.acquireLock();
                }
            }
        }
    }

    @GCFree("GC-free coding using an object-pool in a single-threaded context")
    private void onNewTickEvent(UITickWorkflowEvent event, long sequence, boolean endOfBatch) {
        if(LOGGER.isDebugEnabled()) {
            LOGGER.debug("onNewTickEvent Starts @ [{}] nanos for tick received @ [{}] nanos", DateTimeUtils.nowInNanos(), event.tickWorkflowEventReceiveTimeNanos());
        }
        
        var newTickEvent = tickEventPool.acquire();
        var uiNewTickEventBytes = event.uiNewTickEvent().rewind();
        tickEventUnMarshaller.accept(newTickEvent, uiNewTickEventBytes.value());
        tickEventEnricher.accept(newTickEvent);
        var oldTickEvent = notifier.updateBatchEvent(newTickEvent, event.tickWorkflowEventReceiveTimeNanos());
        if(oldTickEvent != null) tickEventPool.release(oldTickEvent);
        
        if(isBatchingDisabled()) {
            notifier.multicastIfApplicable(newTickEvent);
        } else if(endOfBatch) {
            notifier.multicastAllInBatchAsApplicable();
//            notifier.clearBatchEvents(tickEventPool);
        }
        
        if(LOGGER.isDebugEnabled()) {
            LOGGER.debug("onNewTickEvent Ends @ [{}] nanos for tick received @ [{}] nanos", DateTimeUtils.nowInNanos(), event.tickWorkflowEventReceiveTimeNanos());
        }
    }
    
    private void onTickTimerEvent(UITickWorkflowEvent event, long sequence, boolean endOfBatch) {
        LOGGER.debug("onTickTimerEvent @ [{}] nanos", event.tickWorkflowEventReceiveTimeNanos());
        notifier.multicastAllInBatchAsApplicable();
    }
    
    private void onAddTickStreamSubscriptionEvent(UITickWorkflowEvent event, long sequence, boolean endOfBatch) {
        var command = event.uiTickStreamSubscptionEvent();
        var streamId = command.streamId();
        var channel = command.transportChannel();
        LOGGER.debug("onAddTickStreamSubscriptionEvent For streamId [{}] on channel", streamId);
        streamSubscriptionRegistry.addStreamSubscriptionForUserChannel(streamId, channel);
        boolean unicastResult = notifier.unicast(channel, streamId);
        LOGGER.debug("Unicast Attempt Result For streamId [{}] on channel  : [{}]", streamId,  unicastResult);
    }

    private void onRemoveTickStreamSubscriptionEvent(UITickWorkflowEvent event, long sequence, boolean endOfBatch) {
        var command = event.uiTickStreamSubscptionEvent();
        var streamId = command.streamId();
        var channel = command.transportChannel();
        LOGGER.debug("onRemoveTickStreamSubscriptionEvent For streamId [{}] on channel @ [{}] nanos", streamId, event.tickWorkflowEventReceiveTimeNanos());
        streamSubscriptionRegistry.removeStreamSubscriptionForUserChannel(streamId, channel);
    }

    private void onDisconnectTickStreamSubscriptionChannelEvent(UITickWorkflowEvent event, long sequence, boolean endOfBatch) {
        var command = event.uiTickStreamSubscptionEvent();
        var channel = command.transportChannel();
        LOGGER.debug("onDisconnectTickStreamSubscriptionChannelEvent For channel @ [{}] nanos", event.tickWorkflowEventReceiveTimeNanos());
        streamSubscriptionRegistry.removeDeadChannel(channel);
    }

    private void onTickStreamChannelWriteSuccessEvent(UITickWorkflowEvent event, long sequence, boolean endOfBatch) {
        var writeActionNotification = event.uiTickStreamChannelWriteActionNotificationEvent();
        var channel = writeActionNotification.transportChannel();
        var writeBuffer = writeActionNotification.tempByteBufferToReturn();
        LOGGER.debug("onTickStreamChannelWriteSuccessEvent For channel @ [{}] nanos", event.tickWorkflowEventReceiveTimeNanos());
        notifier.onWriteActionNotifiction(channel, writeBuffer);
    }

    private void onTickStreamChannelWriteErrorEvent(UITickWorkflowEvent event, long sequence, boolean endOfBatch) {
        var writeActionNotification = event.uiTickStreamChannelWriteActionNotificationEvent();
        var channel = writeActionNotification.transportChannel();
        var writeBuffer = writeActionNotification.tempByteBufferToReturn();
        var writeError = writeActionNotification.writeError();
        LOGGER.debug("onTickStreamChannelWriteErrorEvent For channel @ [{}] nanos", event.tickWorkflowEventReceiveTimeNanos());
        streamSubscriptionRegistry.removeDeadChannel(channel);
        notifier.onWriteActionNotifiction(channel, writeBuffer, writeError);
    }

    /*
     * The following STOMP specific methods are reatined temporarily for the legacy STOMP messaging.
     * And it will be removed shortly. 
     */

    private void onMulticastAllToAllUsersOnSTOMPEvent(UITickWorkflowEvent event, long sequence, boolean endOfBatch) {
        LOGGER.debug("onMulticastAllToAllUsersOnSTOMPEvent For channel @ [{}] nanos", event.tickWorkflowEventReceiveTimeNanos());
        notifier.multicastAllAvailableOnSTOMP();
    }
    
    private void onMulticastAllToGivenUserOnSTOMPEvent(UITickWorkflowEvent event, long sequence, boolean endOfBatch) {
        LOGGER.debug("onMulticastAllToGivenUserOnSTOMPEvent For channel @ [{}] nanos", event.tickWorkflowEventReceiveTimeNanos());
        notifier.multicastAllAvailableOnSTOMP(event.userName());
    }
    
    @Setter
    @Accessors(fluent = true, chain = true)
    public static class Builder<E> {
        private Disruptor<UITickWorkflowEvent> uiTickEventDisruptor;
        private UIEventNotifierPlusPlus<E> uiEventNotifier;
        private ObjectPool<E> tickEventPool;
        private BiConsumer<E, Bytes<?>> tickEventUnMarshaller;
        private Consumer<E> tickEventEnricher;
        private boolean isTicksBatchingEnabled = true;
        private long tickInterval;
        private TimeUnit tickIntervalUnit;
        private StreamSubscriptionRegistry streamSubscriptionRegistry;
        private boolean isCorePinningEnabled = false;
        private int pinToCoreCPUId = -1;
        
        public UIEventTickServicePlusPlus<E> build() {
            requireNonNull(uiTickEventDisruptor);
            requireNonNull(uiEventNotifier);
            requireNonNull(tickEventPool);
            requireNonNull(tickEventUnMarshaller);
            requireNonNull(tickEventEnricher);
            checkArgument(tickInterval > 0, "tickInterval must be specified");
            requireNonNull(tickIntervalUnit);
            requireNonNull(streamSubscriptionRegistry);
            return new UIEventTickServicePlusPlus<>(this);
        }
    }
}
