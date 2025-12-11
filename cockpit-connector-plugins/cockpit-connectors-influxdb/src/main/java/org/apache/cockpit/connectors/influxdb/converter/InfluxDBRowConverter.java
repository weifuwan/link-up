package org.apache.cockpit.connectors.influxdb.converter;


import org.apache.cockpit.connectors.api.catalog.exception.CommonErrorCodeDeprecated;
import org.apache.cockpit.connectors.api.type.SeaTunnelDataType;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.api.type.SqlType;
import org.apache.cockpit.connectors.influxdb.exception.InfluxdbConnectorException;

import java.util.ArrayList;
import java.util.List;

public class InfluxDBRowConverter {

    public static SeaTunnelRow convert(
            List<Object> values, SeaTunnelRowType typeInfo, List<Integer> indexList) {

        SeaTunnelDataType<?>[] seaTunnelDataTypes = typeInfo.getFieldTypes();
        List<Object> fields = new ArrayList<>(seaTunnelDataTypes.length);

        for (int i = 0; i <= seaTunnelDataTypes.length - 1; i++) {
            Object seaTunnelField;
            int columnIndex = indexList.get(i);
            SeaTunnelDataType<?> seaTunnelDataType = seaTunnelDataTypes[i];
            SqlType fieldSqlType = seaTunnelDataType.getSqlType();
            if (null == values.get(columnIndex)) {
                seaTunnelField = null;
            } else if (SqlType.BOOLEAN.equals(fieldSqlType)) {
                seaTunnelField = Boolean.parseBoolean(values.get(columnIndex).toString());
            } else if (SqlType.SMALLINT.equals(fieldSqlType)) {
                seaTunnelField = Short.valueOf(values.get(columnIndex).toString());
            } else if (SqlType.INT.equals(fieldSqlType)) {
                seaTunnelField = Integer.valueOf(values.get(columnIndex).toString());
            } else if (SqlType.BIGINT.equals(fieldSqlType)) {
                seaTunnelField = Long.valueOf(values.get(columnIndex).toString());
            } else if (SqlType.FLOAT.equals(fieldSqlType)) {
                seaTunnelField = ((Double) values.get(columnIndex)).floatValue();
            } else if (SqlType.DOUBLE.equals(fieldSqlType)) {
                seaTunnelField = values.get(columnIndex);
            } else if (SqlType.STRING.equals(fieldSqlType)) {
                seaTunnelField = values.get(columnIndex);
            } else {
                throw new InfluxdbConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        "Unsupported data type: " + seaTunnelDataType);
            }

            fields.add(seaTunnelField);
        }

        return new SeaTunnelRow(fields.toArray());
    }
}
