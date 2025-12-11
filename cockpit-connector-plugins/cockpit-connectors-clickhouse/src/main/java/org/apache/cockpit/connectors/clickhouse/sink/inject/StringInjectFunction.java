package org.apache.cockpit.connectors.clickhouse.sink.inject;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cockpit.connectors.api.catalog.exception.CommonError;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class StringInjectFunction implements ClickhouseFieldInjectFunction {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private String fieldType;

    @Override
    public void injectFields(PreparedStatement statement, int index, Object value)
            throws SQLException {
        try {
            if ("Point".equals(fieldType)) {
                statement.setObject(
                        index, MAPPER.readValue(replace(value.toString()), double[].class));
            } else if ("Ring".equals(fieldType)) {
                statement.setObject(
                        index, MAPPER.readValue(replace(value.toString()), double[][].class));
            } else if ("Polygon".equals(fieldType)) {
                statement.setObject(
                        index, MAPPER.readValue(replace(value.toString()), double[][][].class));
            } else if ("MultiPolygon".equals(fieldType)) {
                statement.setObject(
                        index, MAPPER.readValue(replace(value.toString()), double[][][][].class));
            } else {
                statement.setString(index, value.toString());
            }
        } catch (JsonProcessingException e) {
            throw CommonError.jsonOperationError("Clickhouse", value.toString(), e);
        }
    }

    @Override
    public boolean isCurrentFieldType(String fieldType) {
        if ("String".equals(fieldType)
                || "Int128".equals(fieldType)
                || "UInt128".equals(fieldType)
                || "Int256".equals(fieldType)
                || "UInt256".equals(fieldType)
                || "Point".equals(fieldType)
                || "Ring".equals(fieldType)
                || "Polygon".equals(fieldType)
                || "MultiPolygon".equals(fieldType)) {
            this.fieldType = fieldType;
            return true;
        }
        return false;
    }

    private static String replace(String str) {
        return str.replaceAll("\\(", "[").replaceAll("\\)", "]");
    }
}
