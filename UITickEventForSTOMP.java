/**
 * 
 */
package com.intrade.etrading.wsp.common;

import com.intrade.commons.util.functor.Resettable;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author Venny Sahukara
 * TEMPORARY Class and to be removed. Just to support STOMP for legacy reasons.
 */
@Data
@Accessors(fluent = true, chain = true)
public final class UITickEventForSTOMP implements Resettable {
    private String userName = null;
    
    @Override
    public void reset() {
        userName = null;
    }
}
