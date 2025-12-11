package org.apache.cockpit.connectors.api.jdbc.converter;


import org.apache.cockpit.connectors.api.catalog.TableSchema;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Converter that is responsible to convert between JDBC object and SeaTunnel data structure {@link
 * SeaTunnelRow}.
 */
public interface JdbcRowConverter extends Serializable {

    /**
     * Convert data retrieved from {@link ResultSet} to internal {@link SeaTunnelRow}.
     *
     * @param rs ResultSet from JDBC
     */
    SeaTunnelRow toInternal(ResultSet rs, TableSchema tableSchema) throws SQLException;

    @Deprecated
    PreparedStatement toExternal(
            TableSchema tableSchema, SeaTunnelRow row, PreparedStatement statement)
            throws SQLException;

    /**
     * Convert data from internal {@link SeaTunnelRow} to JDBC object.
     */
    default PreparedStatement toExternal(
            TableSchema tableSchema,
            @Nullable TableSchema databaseTableSchema,
            SeaTunnelRow row,
            PreparedStatement statement)
            throws SQLException {
        return toExternal(tableSchema, row, statement);
    }
}
