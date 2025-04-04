/**
 * 
 */
package com.intrade.etrading.wsp.common;

import static net.openhft.chronicle.hash.impl.util.Objects.requireNonNull;

import com.intrade.commons.util.functor.Copyable;
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
public final class UITickStreamSubscriptionEvent implements Resettable, Copyable<UITickStreamSubscriptionEvent> {
    private WebSocketChannel transportChannel = null;
    private StreamCommand streamCommand;
    private String streamId = null;
    
    @Override
    public void reset() {
        transportChannel = null;
        streamCommand = StreamCommand.UNKNOWN;
        streamId = null;
    }
    
    @Override
    public void copy(UITickStreamSubscriptionEvent other) {
        requireNonNull(other);
        transportChannel = other.transportChannel;
        streamCommand = other.streamCommand;
        streamId = other.streamId;
    }
}
