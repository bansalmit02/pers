package com.intrade.etrading.wsp.common;

import java.nio.ByteBuffer;

import com.intrade.commons.util.lifecycle.ManageableService;
import com.intrade.commons.util.values.BytesValue;
import com.intrade.etrading.undertow.handler.StreamCommand;

import io.undertow.websockets.core.WebSocketChannel;

public interface TickWorkflowServicePlusPlus extends ManageableService {

    void publishNewTickEvent(BytesValue tickEvent, long tickReceiveTimeNanos);

    void publishTickTimerEvent();

    //NOT my style - more than 3 method parameters!
    void publishTickStreamSubscriptionCommand(
            WebSocketChannel transportChannel,
            StreamCommand streamCommand,
            String streamId,
            long requestTimestampNanos);

    default void publishTickStreamSubscriptionCommand(WebSocketChannel transportChannel, 
            UITickStreamSubscriptionEvent command,
            long requestTimestampNanos) {
        publishTickStreamSubscriptionCommand(
                transportChannel,
                command.streamCommand(), 
                command.streamId(), 
                requestTimestampNanos);
    }
    
    default void publishClientDisconnect(WebSocketChannel transportChannel, long disconnectTimestampNanos) {
        publishTickStreamSubscriptionCommand(
                transportChannel,
                StreamCommand.DISCONNECT, 
                null, 
                disconnectTimestampNanos);
    }
    
    void publishWriteActionSuccessEvent(
            WebSocketChannel transportChannel,
            ByteBuffer tempBufferToReturn,
            long actionNotificationTimestampNanos);

    void publishWriteActionErrorEvent(
            WebSocketChannel transportChannel,
            ByteBuffer tempBufferToReturn,
            Throwable writeError,
            long actionNotificationTimestampNanos);

    /*
     * TODO: Temp method to support legacy STOMP. To be removed shortly.
     */
    void publishBcastAllToAllUsersOnSTOMPEvent();

    /*
     * TODO: Temp method to support legacy STOMP. To be removed shortly.
     */
    void publishBcastAllToGivenUserOnSTOMPEvent(String userName);
}