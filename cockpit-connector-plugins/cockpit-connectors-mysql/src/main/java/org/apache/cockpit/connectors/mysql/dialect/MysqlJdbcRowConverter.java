package org.apache.cockpit.connectors.mysql.dialect;

import org.apache.cockpit.connectors.api.jdbc.converter.AbstractJdbcRowConverter;
import org.apache.cockpit.connectors.api.jdbc.dialect.DatabaseIdentifier;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class MysqlJdbcRowConverter extends AbstractJdbcRowConverter {
    @Override
    public String converterName() {
        return DatabaseIdentifier.MYSQL;
    }

    @Override
    protected void writeTime(PreparedStatement statement, int index, LocalTime time)
            throws SQLException {
        // Write to time column using timestamp retains milliseconds
        statement.setTimestamp(
                index, java.sql.Timestamp.valueOf(LocalDateTime.of(LocalDate.now(), time)));
    }
}
