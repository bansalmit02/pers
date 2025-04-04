package com.intrade.etrading.wsp.common;

import static com.google.common.base.Preconditions.checkArgument;
import static com.intrade.commons.util.string.StringUtils.isNonBlank;
import static java.util.Objects.requireNonNull;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.collections.Object2ObjectHashMap;
import org.coderrock.commons.messaging.pubsub.chroniclebytes.config.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import com.intrade.commons.codec.json.JsonEncoder;
import com.intrade.commons.codec.json.gson.codecs.JsonCodecs;
import com.intrade.commons.messaging.pubsub.pub.EventPublicationStreamer;
import com.intrade.commons.util.annotation.GCFree;
import com.intrade.commons.util.datetime.DateTimeUtils;
import com.intrade.commons.util.functor.Object2BooleanFunction;
import com.intrade.commons.util.functor.ObjectAndBooleanConsumer;
import com.intrade.commons.util.functor.ObjectAndLongConsumer;
import com.intrade.commons.util.lifecycle.ManageableService;
import com.intrade.commons.util.messaging.event.CoreByteBufferEvent;
import com.intrade.commons.util.pool.ObjectPool;
import com.intrade.commons.util.values.ByteBufferValue;
import com.intrade.etrading.utils.GsonUtils;

import io.undertow.websockets.core.WebSocketChannel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@NotThreadSafe
public class UIEventNotifierPlusPlus<E> implements ManageableService {
    private static final Logger LOGGER = LoggerFactory.getLogger(UIEventNotifierPlusPlus.class);
    
    private volatile boolean isAvailable;
    private final String name;
	private final JsonEncoder jsonEncoder = JsonCodecs.newJsonEncoder(GsonUtils.getNew());//TODO: Inject it from outside
	
	private final NetworkPacketCachePlusPlus<E> networkPacketCache;
    private final int maxPacketSize;

    @Getter
    private final boolean isUndertowEnabled;
    private final UIMessageDistributor<WebSocketChannel, ByteBufferValue> undertowUIMessageDistributor;
    private final EventPublicationStreamer<CoreByteBufferEvent> packetLogger;
	
    private final String destinationTopic;
    private final SimpMessagingTemplate messagingTemplate;

    private final ToLongFunction<E> wspReceiveTimeGetter;
    private final ObjectAndLongConsumer<E> wspReceiveTimeSetter;
    private final ToLongFunction<E> wspPublishTimeGetter;
    private final ObjectAndLongConsumer<E> wspPublishTimeSetter;

    private final ThreadLocal<CoreByteBufferEvent> jsonEventHolder = ThreadLocal.withInitial(WspUtils::newByteBufferEvent);;
	
    private final ThreadLocal<PacketBuildFunction> packetBuildFunctionHolder = ThreadLocal.withInitial(PacketBuildFunction::new);
    
    private final long tickIntervalMillis;
    private final byte tickIntervalThrottlePercentage;
    private final long tickExpiryDurationMillis;
    private final Object2BooleanFunction<E> isExpiredFlagOnEventGetter;
    private final ObjectAndBooleanConsumer<E> isExpiredFlagOnEventSetter;
    private final Supplier<E> blankEventSupplier;
    private final Map<String, E> blankEvents = new Object2ObjectHashMap<>();
    private final BiConsumer<E,E> blankEventCopier;
    private final Function<String, E> blankEventGetter;

    private UIEventNotifierPlusPlus(Builder<E> builder) {
        this.name = builder.name;
        this.networkPacketCache = builder.networkPacketCache;
        this.destinationTopic = builder.destinationTopic;
        this.maxPacketSize = builder.maxPacketSize;
        this.isUndertowEnabled = builder.isUndertowEnabled;
        this.undertowUIMessageDistributor = builder.undertowUIMessageDistributor;
        this.messagingTemplate = builder.messagingTemplate;
        this.packetLogger = builder.packetLogger;
        this.wspReceiveTimeGetter = builder.wspReceiveTimeGetter;
        this.wspReceiveTimeSetter = builder.wspReceiveTimeSetter;
        this.wspPublishTimeGetter = builder.wspPublishTimeGetter;
        this.wspPublishTimeSetter = builder.wspPublishTimeSetter;
        this.tickIntervalMillis = builder.tickIntervalMillis;
        this.tickIntervalThrottlePercentage = builder.tickIntervalThrottlePercentage;
        this.tickExpiryDurationMillis = builder.tickExpiryDurationMillis;
        this.isExpiredFlagOnEventGetter = builder.isExpiredFlagOnEventGetter;
        this.isExpiredFlagOnEventSetter = builder.isExpiredFlagOnEventSetter;
        this.blankEventSupplier = builder.blankEventSupplier;
        this.blankEventCopier = builder.blankEventCopier;
        this.blankEventGetter =  k -> blankEventSupplier.get();
    }
    
    @Override
    public void start() {
        isAvailable = true;
    }
    
    @Override
    public void stop() {
        isAvailable = false;
        networkPacketCache.clearData();
        
    }
    
    @Override
    public boolean isAvailable() {
        return isAvailable;
    }
    
    @Override
    public String getName() {
        return name;
    }

    //Update event as part of batch
    @GCFree("GC-free coding")
    public E updateBatchEvent(E newEvent, long eventReceiveTimeStampNanos) {
        wspReceiveTimeSetter.accept(newEvent, eventReceiveTimeStampNanos);
        var oldEvent = networkPacketCache.updateBatch(newEvent);
        
        //For low-GC
        var packetBuildFunction = packetBuildFunctionHolder.get();
        packetBuildFunction.event(newEvent);
        packetBuildFunction.delegate(packetSettingForBatchUpdate);
        String key = networkPacketCache.getKey(newEvent);
        networkPacketCache.compute(key, packetBuildFunction);
        packetBuildFunction.event(null);
        packetBuildFunction.delegate(null);
        
        return oldEvent;
    }

    @GCFree("GC-free coding")
    public void multicastIfApplicable(E event) {
		try {
			String key = networkPacketCache.getKey(event);
			
		    //For low-GC
			var packetBuildFunction = packetBuildFunctionHolder.get();
			packetBuildFunction.event(event);
            packetBuildFunction.delegate(packetSettingForMultiCast);
            var ntwkPacket = networkPacketCache.compute(key, packetBuildFunction);
            packetBuildFunction.event(null);
            packetBuildFunction.delegate(null);
            
			if(!ntwkPacket.isMulticasted() && !isThrottled(ntwkPacket)) {
			    long currentTimeNanos = DateTimeUtils.nowInNanos();
	            wspPublishTimeSetter.accept(event, currentTimeNanos);
	            long lastPktPublishTimeNanos = ntwkPacket.getPreviousPacketPublishTimeNanos();
	            ntwkPacket.setPreviousPacketPublishTimeNanos(currentTimeNanos);
	            //directly writes to payloadJsonBytes.
	            jsonEncoder.toJson(event, ntwkPacket.getPayloadEventJsonBytes());
		        boolean isMulticasted = multicastPayload(key, ntwkPacket);
                ntwkPacket.setMulticasted(isMulticasted);
                
                if(isMulticasted) {
                    logPacket(ntwkPacket);                
                } else {
                    //Reset the state back to what it was before the unsuccessful multicast attempt!
                    wspPublishTimeSetter.accept(event, 0);
                    ntwkPacket.setPreviousPacketPublishTimeNanos(lastPktPublishTimeNanos);
                    ntwkPacket.getPayloadEventJsonBytes().rewind();
                }
			} else {
				LOGGER.debug("Skipping to broadcast data JSON as it was already broadcasted");
			}
		} catch (Exception e) {
			LOGGER.error("Exception while broadcasting data json",e);
		}
	}
    
    public void multicastAllInBatchAsApplicable() {
        //low-GC iterator as argona collection
        long currentTimestamp = DateTimeUtils.nowInNanos();
        for(E event : networkPacketCache.getAllBatchedData()) {
            if(isNotExpired(event, currentTimestamp)) {
                multicastIfApplicable(event);
            } else if(!isMarkedAsExpired(event)) {
                LOGGER.debug("Expired, tick: {}", event);
                String streamId = networkPacketCache.getKey(event);
                var blankEvent = blankEvents.computeIfAbsent(streamId, blankEventGetter);
                blankEventCopier.accept(event, blankEvent);
                LOGGER.debug("Publishing blank tick: {}", blankEvent);
                multicastIfApplicable(blankEvent);
                markAsExpired(event);
            }
        }
    }
 
    public void multicast(String streamId) {
        try {
            var ntwkPacket = networkPacketCache.getNetworkPacket(streamId);
            if(ntwkPacket != null) multicastPayload(streamId, ntwkPacket);
        } catch (Exception e) {
            LOGGER.error("Exception while multicasting data json",e);
        }
    }
   
    public boolean unicast(WebSocketChannel channel, String streamId) {
        try {
            var ntwkPacket = networkPacketCache.getNetworkPacket(streamId);
            if(ntwkPacket != null) return unicastPayload(channel, streamId, ntwkPacket);
        } catch (Exception e) {
            LOGGER.error("Exception while multicasting data json",e);
        }
        return false;
    }

    public void onWriteActionNotifiction(WebSocketChannel channel, ByteBuffer writeBuffer, Throwable writeError) {
        undertowUIMessageDistributor.onWriteActionNotifiction(channel, writeBuffer, writeError);
    }
    
    public void onWriteActionNotifiction(WebSocketChannel channel, ByteBuffer writeBuffer) {
        undertowUIMessageDistributor.onWriteActionNotifiction(channel, writeBuffer);
    }

    /*
     * The following will NOT be optimized any further as it meant for the legacy STOMP messaging.
     * And it will be removed further. 
     */

    public void multicastAllAvailableOnSTOMP() {
        try {
            Collection<NetworkPacket<E>> networkPackets = networkPacketCache.getAllNetworkPackets();
            for(var packet: networkPackets) {
                messagingTemplate.convertAndSend(destinationTopic, packet.getPayloadEventJson());
            }
            LOGGER.debug("Notifying cached Snapshot to ALL, size : {}", networkPackets.size());

        }catch (Exception e) {
            LOGGER.error("",e);
        }
    }

    public void multicastAllAvailableOnSTOMP(String userName) {
        try {
            Collection<NetworkPacket<E>> networkPackets = networkPacketCache.getAllNetworkPackets();
            for(var packet: networkPackets) {
                messagingTemplate.convertAndSendToUser(userName, destinationTopic, packet.getPayloadEventJson());
            }
            LOGGER.debug("Notifying cached Snapshots to user {} , size : {}", userName, networkPackets.size());

        }catch (Exception e) {
            LOGGER.error("",e);
        }
    }

    private boolean isThrottled(NetworkPacket<E> packet) {
        long nowNanos = DateTimeUtils.nowInNanos();
        long prevPktPublishTimeNanos = packet.getPreviousPacketPublishTimeNanos();
        
        var timeSinceLastPktSentInMillis = ((nowNanos - prevPktPublishTimeNanos) / 1000000);
        return timeSinceLastPktSentInMillis < (tickIntervalThrottlePercentage * tickIntervalMillis)/100;
    }
    
    private long tickAgeInMillis(E event, long nowNanos) {
        var tickReceivedTimeNanos = wspReceiveTimeGetter.applyAsLong(event);
        var tickAgeInMillis = ((nowNanos - tickReceivedTimeNanos) / 1000000);
        return tickAgeInMillis;
    }
    
    private boolean isNotExpired(E event, long nowNanos) {
        return tickAgeInMillis(event, nowNanos) <= tickExpiryDurationMillis;
    }

    private boolean isMarkedAsExpired(E event) {
        return isExpiredFlagOnEventGetter.applyAsBoolean(event);
    }
    
    private void markAsExpired(E event) {
        isExpiredFlagOnEventSetter.accept(event, true);
    }
    
    public void clearBatchEvents(ObjectPool<E> owingPool) {
        networkPacketCache.clear(owingPool);
    }
    
    
    @GCFree("GC-free coding")
    private final BiFunction<E, NetworkPacket<E>, NetworkPacket<E>> packetSettingForBatchUpdate = (event, packet) -> packetSettingForBatchUpdate(event, packet);
    @GCFree("GC-free coding")
    private final BiFunction<E, NetworkPacket<E>, NetworkPacket<E>> packetSettingForMultiCast = (event, packet) -> packetSettingForMultiCast(event, packet);

    //For low-GC
    @Setter
    @Accessors(fluent = true)
    private class PacketBuildFunction implements BiFunction<String, NetworkPacket<E>, NetworkPacket<E>> {
        private E event;
        private BiFunction<E, NetworkPacket<E>, NetworkPacket<E>> delegate;
        
        @Override
        public NetworkPacket<E> apply(String k, NetworkPacket<E> packet) {
            return delegate.apply(event, packet);
        }
    }
    
    private NetworkPacket<E> packetSettingForBatchUpdate(E event, NetworkPacket<E> packet) {
        if(packet != null && isNewerEvent(event, packet)) {
            setupPacketWithNewEvent(event, packet);
        }
        return packet;
    }
    
    private NetworkPacket<E> packetSettingForMultiCast(E event, NetworkPacket<E> packet) {
        if(packet == null) {
            packet = WspUtils.newNetworkPacket(maxPacketSize);
        }
        
        if(isNewerEvent(event, packet)) {
            setupPacketWithNewEvent(event, packet);
        }

        return packet;
    }
    
    private void setupPacketWithNewEvent(E event, NetworkPacket<E> packet) {
        var previousPktPublishTimeNanos = packet.getPreviousPacketPublishTimeNanos();
        packet.reset();
        packet.setPreviousPacketPublishTimeNanos(previousPktPublishTimeNanos);
        packet.setPayloadEvent(event);
        packet.setMulticasted(false);
    } 
    
    private boolean isNewerEvent(E event, NetworkPacket<E> packet) {
        var eventOnPacket = packet.getPayloadEvent();
        var eventOnPacketUIPublishTime = 0L;
        if(eventOnPacket != null) eventOnPacketUIPublishTime = wspPublishTimeGetter.applyAsLong(eventOnPacket);
        var eventUIPublishTime = wspPublishTimeGetter.applyAsLong(event);
        
        return eventOnPacketUIPublishTime <= 0 || eventOnPacket != event || eventOnPacketUIPublishTime != eventUIPublishTime;
    }
    
    private boolean multicastPayload(String key, NetworkPacket<E> ntwkPacket) {
        var jsonBytes = ntwkPacket.getPayloadEventJsonBytes();
        if(isUndertowEnabled()) {
            return undertowUIMessageDistributor.multicast(key, jsonBytes);
        } else {
            messagingTemplate.convertAndSend(destinationTopic, ntwkPacket.getPayloadEventJson());
            return true;
        }
    }

    private boolean unicastPayload(WebSocketChannel channel, String key, NetworkPacket<E> ntwkPacket) {
        if(!ntwkPacket.isMulticasted() && ntwkPacket.getPayloadEvent()!=null) {
            jsonEncoder.toJson(ntwkPacket.getPayloadEvent(), ntwkPacket.getPayloadEventJsonBytes());
        }
        var jsonBytes = ntwkPacket.getPayloadEventJsonBytes();
        if(isUndertowEnabled() && ntwkPacket.getPayloadEvent()!=null) {
            return undertowUIMessageDistributor.unicast(channel, jsonBytes);
        } else if(ntwkPacket.getPayloadEvent()==null) {
            LOGGER.debug("No payload event available on the networkPacket to be sent!!");
        }
        return false;
    }

    private void logPacket(NetworkPacket<E> ntwkPacket) {
        var jsonBytes = ntwkPacket.getPayloadEventJsonBytes();              
        var jsonEvent = jsonEventHolder.get();
        jsonEvent.setBody(jsonBytes.rewind());              
        packetLogger.publish(jsonEvent);
        jsonBytes.rewind();
        jsonEvent.setBody(null);
    }
    
    @Setter
    @Accessors(fluent = true, chain = true)
    public static class Builder<E> {
        private String name = "DefaultUIEventNotifier";
        private NetworkPacketCachePlusPlus<E> networkPacketCache;
        private String destinationTopic;
        private int maxPacketSize = Constants.MAX_MSG_SIZE_BYTES*4;
        private boolean isUndertowEnabled;
        private UIMessageDistributor<WebSocketChannel, ByteBufferValue> undertowUIMessageDistributor;
        private SimpMessagingTemplate messagingTemplate;
        private EventPublicationStreamer<CoreByteBufferEvent> packetLogger;
        private ToLongFunction<E> wspReceiveTimeGetter;
        private ObjectAndLongConsumer<E> wspReceiveTimeSetter;
        private ToLongFunction<E> wspPublishTimeGetter;
        private ObjectAndLongConsumer<E> wspPublishTimeSetter;
        private long tickIntervalMillis;
        private byte tickIntervalThrottlePercentage;
        private long tickExpiryDurationMillis;
        private Object2BooleanFunction<E> isExpiredFlagOnEventGetter;
        private ObjectAndBooleanConsumer<E> isExpiredFlagOnEventSetter;
        private Supplier<E> blankEventSupplier;
        private BiConsumer<E,E> blankEventCopier;
        
        public UIEventNotifierPlusPlus<E> build() {
            requireNonNull(networkPacketCache, "networkPacketCache must be specified");
            checkArgument(isNonBlank(destinationTopic), "destinationTopic must be sepcified");
            //Temporarily commented undertowUIMessageDistributor can be null when under is disabled!
//            requireNonNull(undertowUIMessageDistributor, "undertowUIMessageDistributor must be specified");
//            requireNonNull(messagingTemplate, "messagingTemplate must be specified");
            checkArgument(maxPacketSize > 0, "maxPacketSize must be sepcified");
            requireNonNull(packetLogger, "packetLogger must be specified");

            requireNonNull(wspReceiveTimeGetter, "uiReceiveTimeGetter must be specified");
            requireNonNull(wspReceiveTimeSetter, "uiReceiveTimeSetter must be specified");
            requireNonNull(wspPublishTimeGetter, "uiPublishTimeGetter must be specified");
            requireNonNull(wspPublishTimeSetter, "uiPublishTimeSetter must be specified");
            
            checkArgument(tickIntervalMillis > 0, "tickIntervalMillis must be sepcified");
            checkArgument(tickIntervalThrottlePercentage >= 0, "tickIntervalThrottlePercentage must be sepcified");
            checkArgument(tickExpiryDurationMillis > 0, "tickExpiryDurationMillis must be sepcified");
            requireNonNull(isExpiredFlagOnEventGetter, "isExpiredFlagOnEventGetter must be specified");
            requireNonNull(isExpiredFlagOnEventSetter, "isExpiredFlagOnEventSetter must be specified");
            
            requireNonNull(blankEventSupplier, "blankEventSupplier must be specified");
            requireNonNull(blankEventCopier, "blankEventCopier must be specified");

            return new UIEventNotifierPlusPlus<>(this);
        }
        
    }
}
