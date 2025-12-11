package org.apache.cockpit.connectors.clickhouse.sink.inject;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DoubleInjectFunction implements ClickhouseFieldInjectFunction {
    @Override
    public void injectFields(PreparedStatement statement, int index, Object value)
            throws SQLException {
        if (value instanceof BigDecimal) {
            statement.setDouble(index, ((BigDecimal) value).doubleValue());
        } else {
            statement.setDouble(index, Double.parseDouble(value.toString()));
        }
    }

    @Override
    public boolean isCurrentFieldType(String fieldType) {
        return "Float64".equals(fieldType);
    }
}
