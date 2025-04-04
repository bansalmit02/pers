/**
 * 
 */
package com.intrade.etrading.wsp.common;

import lombok.Getter;

/**
 * @author Venny Sahukara
 *
 */
@Getter
public enum UITickWorkflowEventType {
    UNDEFINED_EVENT((byte)0),
    NEW_TICK_EVENT((byte)1),
    TICK_TIMER_EVENT((byte)2),
    
    ADD_TICK_STREAM_SUBSCRIPTION_EVENT((byte)3),
    REMOVE_TICK_STREAM_SUBSCRIPTION_EVENT((byte)4),
    TICK_STREAM_SUBSCRIPTION_CHANNEL_DISCONNECT_EVENT((byte)5),
    
    TICK_STREAM_SUBSCRIPTION_CHANNEL_WRITE_SUCCESS_EVENT((byte)6),
    TICK_STREAM_SUBSCRIPTION_CHANNEL_WRITE_ERROR_EVENT((byte)7),

    //TODO: Temp Types : The following will eventually be removed!!!
    BCAST_ALL_TO_ALL_USERS_ON_STOMP((byte)8),
    BCAST_ALL_TO_GIVEN_USER_ON_STOMP((byte)9);
    
    private final byte code;
    
    private UITickWorkflowEventType(byte code) {
        this.code = code;
    }
    
    public static UITickWorkflowEventType fromCode(byte code) {
        return values()[code];
    }
}
