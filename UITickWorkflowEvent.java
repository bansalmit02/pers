/**
 * 
 */
package com.intrade.etrading.wsp.common;

import java.nio.ByteBuffer;

import com.intrade.commons.util.functor.Resettable;
import com.intrade.etrading.undertow.handler.StreamCommand;

import io.undertow.websockets.core.WebSocketChannel;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author Venny Sahukara
 *
 */
@Data
@Accessors(fluent = true, chain = true)
public final class UITickWorkflowEvent implements Resettable {
    private UITickWorkflowEventType tickWorkflowEventType = UITickWorkflowEventType.UNDEFINED_EVENT;
    private long tickWorkflowEventReceiveTimeNanos = -1;
    
    private final UITickStreamSubscriptionEvent uiTickStreamSubscptionEvent = new UITickStreamSubscriptionEvent();
    private final UINewTickEvent uiNewTickEvent;
    private final UITickStreamChannelWriteActionNotificationEvent uiTickStreamChannelWriteActionNotificationEvent = new UITickStreamChannelWriteActionNotificationEvent();
    private final UITickEventForSTOMP uiTickEventForSTOMP = new UITickEventForSTOMP(); //TODO: Only temporary, retained for legacy reasons
    
    public UITickWorkflowEvent() {
        this(6*1024);
    }
    
    public UITickWorkflowEvent(int tickBytesInitialCapacity) {
        this.uiNewTickEvent = new UINewTickEvent(tickBytesInitialCapacity);
    }
    
    @Override
    public void reset() {
        tickWorkflowEventType = UITickWorkflowEventType.UNDEFINED_EVENT;
        tickWorkflowEventReceiveTimeNanos = -1;
        uiTickStreamSubscptionEvent.reset();
        uiNewTickEvent.reset();
        uiTickEventForSTOMP.reset();
    }
    
    //NOT my style - more than 3 method parameters!
    public UITickWorkflowEvent uiTickStreamSubscriptionEvent(
            WebSocketChannel transportChannel,
            StreamCommand streamCommand,
            String streamId,
            long requestTimestampNanos) {
        
        uiTickStreamSubscptionEvent.transportChannel(transportChannel)
            .streamCommand(streamCommand)
            .streamId(streamId);
        return tickWorkflowEventReceiveTimeNanos(requestTimestampNanos);
    }
    
    public UITickWorkflowEvent uiTickStreamSubscriptionEvent(UITickStreamSubscriptionEvent command) {
        uiTickStreamSubscptionEvent.copy(command);
        return this;
    }

    public UITickWorkflowEvent uiTickStreamChannelWriteActionNotificationEvent(
            WebSocketChannel transportChannel,
            ByteBuffer tempByteBufferToReturn,
            long actionNotificationTimestampNanos) {
        return uiTickStreamChannelWriteActionNotificationEvent(transportChannel, 
                tempByteBufferToReturn, null, actionNotificationTimestampNanos);
    }

    public UITickWorkflowEvent uiTickStreamChannelWriteActionNotificationEvent(
            WebSocketChannel transportChannel,
            ByteBuffer tempByteBufferToReturn,
            Throwable writeError,
            long actionNotificationTimestampNanos) {
        uiTickStreamChannelWriteActionNotificationEvent.transportChannel(transportChannel)
            .tempByteBufferToReturn(tempByteBufferToReturn)
            .writeError(writeError);
        return tickWorkflowEventReceiveTimeNanos(actionNotificationTimestampNanos);
    }

    public UITickWorkflowEvent uiTickWorkflowEventType(UITickWorkflowEventType tickEventType) {
        this.tickWorkflowEventType = tickEventType;
        return this;
    }

    public UITickWorkflowEvent uiTickWorkflowEventType(StreamCommand streamCommand) {
        return uiTickWorkflowEventType(toUITickWorkflowEventType(streamCommand));
    }

    public UITickWorkflowEvent userName(String userName) {
        uiTickEventForSTOMP.userName(userName);
        return this;
    }
    
    public String userName() {
        return uiTickEventForSTOMP.userName();
    }
    
    private UITickWorkflowEventType toUITickWorkflowEventType(StreamCommand streamCommand) {
        return switch(streamCommand) {
            case START -> UITickWorkflowEventType.ADD_TICK_STREAM_SUBSCRIPTION_EVENT;
            case STOP -> UITickWorkflowEventType.REMOVE_TICK_STREAM_SUBSCRIPTION_EVENT;
            case DISCONNECT -> UITickWorkflowEventType.TICK_STREAM_SUBSCRIPTION_CHANNEL_DISCONNECT_EVENT;
            case UNKNOWN -> UITickWorkflowEventType.UNDEFINED_EVENT;
        };
    }
}
