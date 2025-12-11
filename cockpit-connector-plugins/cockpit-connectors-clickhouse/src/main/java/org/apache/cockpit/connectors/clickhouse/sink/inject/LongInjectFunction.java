package org.apache.cockpit.connectors.clickhouse.sink.inject;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class LongInjectFunction implements ClickhouseFieldInjectFunction {

    @Override
    public void injectFields(PreparedStatement statement, int index, Object value)
            throws SQLException {
        statement.setLong(index, Long.parseLong(value.toString()));
    }

    @Override
    public boolean isCurrentFieldType(String fieldType) {
        return "UInt32".equals(fieldType)
                || "UInt64".equals(fieldType)
                || "Int64".equals(fieldType)
                || "IntervalYear".equals(fieldType)
                || "IntervalQuarter".equals(fieldType)
                || "IntervalMonth".equals(fieldType)
                || "IntervalWeek".equals(fieldType)
                || "IntervalDay".equals(fieldType)
                || "IntervalHour".equals(fieldType)
                || "IntervalMinute".equals(fieldType)
                || "IntervalSecond".equals(fieldType);
    }
}
