package org.apache.cockpit.connectors.clickhouse.sink.inject;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class IntInjectFunction implements ClickhouseFieldInjectFunction {
    @Override
    public void injectFields(PreparedStatement statement, int index, Object value)
            throws SQLException {
        if (value instanceof Byte) {
            statement.setByte(index, (Byte) value);

        } else if (value instanceof Short) {
            statement.setShort(index, (Short) value);

        } else {
            statement.setInt(index, (Integer) value);
        }
    }

    @Override
    public boolean isCurrentFieldType(String fieldType) {
        return "Int8".equals(fieldType)
                || "UInt8".equals(fieldType)
                || "Int16".equals(fieldType)
                || "UInt16".equals(fieldType)
                || "Int32".equals(fieldType);
    }
}
