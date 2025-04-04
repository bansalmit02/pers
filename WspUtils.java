/**
 * 
 */
package com.intrade.etrading.wsp.common;

import com.intrade.commons.util.messaging.event.CoreByteBufferEvent;
import com.intrade.commons.util.messaging.event.CoreCharSequenceEvent;
import com.intrade.commons.util.messaging.event.CoreEventHeader;
import com.intrade.commons.util.string.StringUtils;
import com.intrade.commons.util.values.ByteBufferValue;
import com.intrade.commons.util.values.CharSequenceValue;
import com.intrade.etrading.marketdata.model.marketdata.L2BookSnapshot;

/**
 * @author Venny Sahukara
 *
 */
public enum WspUtils {
    ;

    public static <T> NetworkPacket<T> newNetworkPacket(int maxLen) {
        var packet = new NetworkPacket<T>();
        var jsonBytes = new ByteBufferValue(maxLen);
        packet.setPayloadEventJsonBytes(jsonBytes);
        return packet;
    }
    
    public static CoreByteBufferEvent newByteBufferEvent() {
        var event = new CoreByteBufferEvent(new CoreEventHeader(), new ByteBufferValue());
        event.setHeaderIncludedInPayload(false);
        return event;
    }

    public static CoreCharSequenceEvent newCoreCharSequenceEvent() {
        var event = new CoreCharSequenceEvent(new CoreEventHeader(), new CharSequenceValue());
        event.setHeaderIncludedInPayload(false);
        return event;
    }
    
    public static long getWspReceiveTime(L2BookSnapshot<?> l2s) {
        return l2s.getMarketDataTimestamps().getWspReceiveTime();
    }
    
    public static void setWspReceiveTime(L2BookSnapshot<?> l2s, long nanos) {
        l2s.getMarketDataTimestamps().setWspReceiveTime(nanos);
    }
    
    public static long getWspPublishTime(L2BookSnapshot<?> l2s) {
        return l2s.getMarketDataTimestamps().getWspPublishTime();
    }
    
    public static void setWspPublishTime(L2BookSnapshot<?> l2s, long nanos) {
        l2s.getMarketDataTimestamps().setWspPublishTime(nanos);
    }
    
    public static String getKey(L2BookSnapshot<?> l2s) {
        String key = StringUtils.isNonBlank(l2s.getPriceStreamId()) ? 
                l2s.getPriceStreamId() : 
                    L2BookSnapshot.buildPriceStreamID(l2s);
        return key;
    }
}
