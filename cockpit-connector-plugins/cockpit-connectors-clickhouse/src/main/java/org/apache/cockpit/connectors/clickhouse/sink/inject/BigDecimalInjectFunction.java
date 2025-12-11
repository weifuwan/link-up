package org.apache.cockpit.connectors.clickhouse.sink.inject;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.regex.Pattern;

public class BigDecimalInjectFunction implements ClickhouseFieldInjectFunction {

    private static final Pattern PATTERN = Pattern.compile("(Decimal.*)");

    @Override
    public void injectFields(PreparedStatement statement, int index, Object value)
            throws SQLException {
        statement.setBigDecimal(index, (java.math.BigDecimal) value);
    }

    @Override
    public boolean isCurrentFieldType(String fieldType) {
        return PATTERN.matcher(fieldType).matches();
    }
}
