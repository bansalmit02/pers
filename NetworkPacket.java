package com.intrade.etrading.wsp.common;

import com.intrade.commons.util.bytes.ByteBufferUtils;
import com.intrade.commons.util.functor.Resettable;
import com.intrade.commons.util.values.ByteBufferValue;

import lombok.Data;

@Data
public class NetworkPacket<E> implements Resettable {
    private E payloadEvent;
    private String payloadEventJson; //TODO: Retained temporarily to support for the stomp websockets
    private ByteBufferValue payloadEventJsonBytes;
    private boolean isMulticasted = false;
    private long previousPacketPublishTimeNanos = 0;
    
    public ByteBufferValue getPayloadEventJsonBytes() {
        return payloadEventJsonBytes != null ? payloadEventJsonBytes.rewind() : payloadEventJsonBytes;
    }
    
    public void setPayloadEventJsonBytes(ByteBufferValue bytes) {
        this.payloadEventJsonBytes = bytes;
        this.payloadEventJson = null;
    }
    
    //TODO: Retained temporarily to support for the existing legacy STOMP websockets
    public String getPayloadEventJson() {
        if (payloadEventJson != null) return payloadEventJson;
        if(payloadEventJsonBytes != null) {
            payloadEventJson = ByteBufferUtils.decodeUtf8(payloadEventJsonBytes.value());
            payloadEventJsonBytes.rewind();
        }
        return payloadEventJson;
    }
    
    @Override
    public void reset() {
        if (payloadEventJsonBytes != null) payloadEventJsonBytes.reset();
        this.payloadEventJson = null;
        this.isMulticasted=false;
    }
}
