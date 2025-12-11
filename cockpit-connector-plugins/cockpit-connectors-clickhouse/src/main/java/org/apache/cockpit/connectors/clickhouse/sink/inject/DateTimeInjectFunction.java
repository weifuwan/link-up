package org.apache.cockpit.connectors.clickhouse.sink.inject;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.regex.Pattern;

public class DateTimeInjectFunction implements ClickhouseFieldInjectFunction {

    private static final Pattern PATTERN = Pattern.compile("(DateTime.*)");

    @Override
    public void injectFields(PreparedStatement statement, int index, Object value)
            throws SQLException {
        if (value instanceof Timestamp) {
            statement.setTimestamp(index, (Timestamp) value);
        } else if (value instanceof LocalDateTime) {
            statement.setObject(index, value);
        } else {
            statement.setTimestamp(index, Timestamp.valueOf(value.toString()));
        }
    }

    @Override
    public boolean isCurrentFieldType(String fieldType) {
        return PATTERN.matcher(fieldType).matches();
    }
}
