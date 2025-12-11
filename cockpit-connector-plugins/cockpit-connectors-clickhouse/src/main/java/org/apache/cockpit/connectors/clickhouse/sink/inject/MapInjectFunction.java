package org.apache.cockpit.connectors.clickhouse.sink.inject;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.regex.Pattern;

public class MapInjectFunction implements ClickhouseFieldInjectFunction {

    private static final Pattern PATTERN = Pattern.compile("(Map.*)");

    @Override
    public void injectFields(PreparedStatement statement, int index, Object value)
            throws SQLException {
        statement.setObject(index, value);
    }

    @Override
    public boolean isCurrentFieldType(String fieldType) {
        return PATTERN.matcher(fieldType).matches();
    }
}
