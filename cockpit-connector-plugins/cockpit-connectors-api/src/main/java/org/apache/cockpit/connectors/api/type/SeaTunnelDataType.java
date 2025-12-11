package org.apache.cockpit.connectors.api.type;

import java.io.Serializable;

/** Logic data type of column in SeaTunnel. */
public interface SeaTunnelDataType<T> extends Serializable {

    /** Gets the class of the type represented by this data type. */
    Class<T> getTypeClass();

    /** Gets the SQL standard type represented by this data type. */
    SqlType getSqlType();
}
