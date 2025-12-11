package org.apache.cockpit.connectors.api.factory;


import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelAPIErrorCode;
import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelRuntimeException;

public class FactoryException extends SeaTunnelRuntimeException {


    public FactoryException(String message, Throwable cause) {
        super(SeaTunnelAPIErrorCode.FACTORY_INITIALIZE_FAILED, message, cause);
    }

    public FactoryException(String message) {
        super(SeaTunnelAPIErrorCode.FACTORY_INITIALIZE_FAILED, message);
    }
}
