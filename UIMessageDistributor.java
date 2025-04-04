package com.intrade.etrading.wsp.common;

import java.nio.ByteBuffer;

public interface UIMessageDistributor<CH, PL> {
    boolean multicast(String streamId, PL payload);
    boolean unicast(CH channel, PL payload);
    void onWriteActionNotifiction(CH channel, ByteBuffer writeBuffer, Throwable error);
    default void onWriteActionNotifiction(CH channel, ByteBuffer writeBuffer) {
        onWriteActionNotifiction(channel, writeBuffer, null);
    }
    
    @SuppressWarnings("unchecked")
    static <CH,PL> UIMessageDistributor<CH, PL> noop() {
        return (UIMessageDistributor<CH, PL>)noop;
    } 

    UIMessageDistributor<?, ?> noop = 
        new UIMessageDistributor<Object, Object>() {
            @Override
            public boolean multicast(String streamId, Object payload) {
                //no-op
                return false;
            }
            
            @Override
            public boolean unicast(Object channel, Object payload) {
                //no-op
                return false;
            }
            
            @Override
            public void onWriteActionNotifiction(Object channel, ByteBuffer writeBuffer, Throwable error) {
                //no-op
            }
        };
}
