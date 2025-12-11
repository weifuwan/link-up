package org.apache.cockpit.connectors.clickhouse.sink.inject;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Injects a field into a ClickHouse statement, used to transform a java type into a ClickHouse
 * type.
 */
public interface ClickhouseFieldInjectFunction extends Serializable {

    /**
     * Inject the value into the statement.
     *
     * @param statement statement to inject into
     * @param value value to inject
     * @param index index in the statement
     */
    void injectFields(PreparedStatement statement, int index, Object value) throws SQLException;

    /**
     * If the fieldType need to be injected by the current function.
     *
     * @param fieldType field type to inject
     * @return true if the fieldType need to be injected by the current function
     */
    boolean isCurrentFieldType(String fieldType);
}
