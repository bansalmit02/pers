/**
 * 
 */
package com.intrade.etrading.wsp.common;

import java.nio.ByteBuffer;

import com.intrade.commons.util.functor.Resettable;

import io.undertow.websockets.core.WebSocketChannel;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author Venny Sahukara
 */
@Data
@Accessors(fluent = true, chain = true)
public final class UITickStreamChannelWriteActionNotificationEvent implements Resettable {
    private WebSocketChannel transportChannel = null;
    private ByteBuffer tempByteBufferToReturn = null;
    private Throwable writeError = null;
    
    @Override
    public void reset() {
        transportChannel = null;
        tempByteBufferToReturn = null;
        writeError = null;
    }
}
