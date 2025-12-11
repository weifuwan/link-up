package org.apache.cockpit.connectors.clickhouse.sink.inject;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class FloatInjectFunction implements ClickhouseFieldInjectFunction {
    @Override
    public void injectFields(PreparedStatement statement, int index, Object value)
            throws SQLException {
        if (value instanceof BigDecimal) {
            statement.setFloat(index, ((BigDecimal) value).floatValue());
        } else {
            statement.setFloat(index, Float.parseFloat(value.toString()));
        }
    }

    @Override
    public boolean isCurrentFieldType(String fieldType) {
        return "Float32".equals(fieldType);
    }
}
