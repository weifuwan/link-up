package org.apache.cockpit.connectors.clickhouse.sink.inject;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DateInjectFunction implements ClickhouseFieldInjectFunction {
    @Override
    public void injectFields(PreparedStatement statement, int index, Object value)
            throws SQLException {
        if (value instanceof Date) {
            statement.setDate(index, (Date) value);
        } else {
            statement.setDate(index, Date.valueOf(value.toString()));
        }
    }

    @Override
    public boolean isCurrentFieldType(String fieldType) {
        return "Date".equals(fieldType);
    }
}
