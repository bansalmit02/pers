/**
 * 
 */
package com.intrade.etrading.wsp.common;

import static net.openhft.chronicle.hash.impl.util.Objects.requireNonNull;

import com.intrade.commons.util.functor.Resettable;
import com.intrade.commons.util.values.BytesValue;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author Venny Sahukara
 *
 */
@Data
@Accessors(fluent = true, chain = true)
public final class UINewTickEvent implements Resettable {
    private final BytesValue eventBytes;
    
    public UINewTickEvent() {
        this(6*1024);
    }
    
    public UINewTickEvent(int initialCapabity) {
        this.eventBytes = new BytesValue(initialCapabity);
    }
    
    @Override
    public void reset() {
        eventBytes.clear();
    }
    
    public BytesValue rewind() {
        return eventBytes.rewind();
    }
    
    public void copyEventBytes(BytesValue otherEventBytes) {
        requireNonNull(otherEventBytes);
        writeBytesIfPresent(otherEventBytes, eventBytes);
        rewind();
    }
    
    private void writeBytesIfPresent(BytesValue fromBytes, BytesValue toBytes) {
        toBytes.clear();
        if (fromBytes != null && !fromBytes.isEmpty()) {
            toBytes.copy(fromBytes);
        }
    }
}
