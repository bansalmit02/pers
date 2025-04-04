package com.intrade.etrading.wsp.common;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.collections.Object2ObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.intrade.commons.util.pool.ObjectPool;

@NotThreadSafe
public class NetworkPacketCachePlusPlus<T> {
	
	@SuppressWarnings("unused")
    private static final Logger LOGGER = LoggerFactory.getLogger(NetworkPacketCachePlusPlus.class);

    private final Map<String, T> dataBatchStore = new Object2ObjectHashMap<>();
	private final Map<String, NetworkPacket<T>> ntwkPacketStore = new Object2ObjectHashMap<>();
	
	private final Function<T, String> keyGetter;
	
	public NetworkPacketCachePlusPlus(Function<T, String> keyGetter) {
	    this.keyGetter = keyGetter;
	}
	
	public String getKey(T data) {
		return keyGetter.apply(data);
	}
	
	public T updateBatch(T data) {
		String key = getKey(data);
		var oldData = dataBatchStore.put(key, data);
        return oldData;
	}
	
	public boolean containsKey(String key) {
		return ntwkPacketStore.containsKey(key);
	}
	
	public T getDataFromBatch(String key) {
		return dataBatchStore.get(key);
	}
	
	public NetworkPacket<T> getNetworkPacket(String key) {
		return ntwkPacketStore.get(key);
	}
	
    public NetworkPacket<T> put(String key, NetworkPacket<T> packet) {
        return ntwkPacketStore.put(key, packet);
    }
	
    public Collection<T> getAllBatchedData(){
        return dataBatchStore.values();
    }

    public Set<Map.Entry<String, T>> getAllBatchedDataEntries(){
		return dataBatchStore.entrySet();
	}

	public Collection<NetworkPacket<T>> getAllNetworkPackets(){
		return ntwkPacketStore.values();
	}

	public NetworkPacket<T> computeIfAbsent(String key, Function<String, NetworkPacket<T>> computeFunction) {
		return ntwkPacketStore.computeIfAbsent(key, computeFunction);
	}

	public NetworkPacket<T> computeIfPresent(String key, BiFunction<String, NetworkPacket<T>, NetworkPacket<T>> computeFunction) {
		return ntwkPacketStore.computeIfPresent(key, computeFunction);
	}

	public NetworkPacket<T> compute(String key, BiFunction<String, NetworkPacket<T>, NetworkPacket<T>> computeFunction) {
		return ntwkPacketStore.compute(key, computeFunction);
	}
	
    public void clear(ObjectPool<T> owingPool) {
        clearData(owingPool);
        clearNetworkPackets(owingPool);
    }
    
    public void clearData(ObjectPool<T> owingPool) {
        dataBatchStore.forEach((key, data) -> owingPool.release(data));
        dataBatchStore.clear();
    }
    
    public void clearNetworkPackets(ObjectPool<T> owingPool) {
        ntwkPacketStore.forEach((key, packet) -> {
            if(packet.getPayloadEvent() != null) owingPool.release(packet.getPayloadEvent());   
        });
        ntwkPacketStore.clear();
    }
	
	public void clear() {
	    clearData();
	    clearNetworkPackets();
	}
	
	public void clearData() {
	    dataBatchStore.clear();
	}
	
	public void clearNetworkPackets() {
	    ntwkPacketStore.clear();
	}
}
