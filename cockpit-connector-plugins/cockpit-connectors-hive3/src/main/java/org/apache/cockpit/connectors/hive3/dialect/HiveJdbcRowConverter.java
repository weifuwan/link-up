package org.apache.cockpit.connectors.hive3.dialect;


import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.exception.CommonError;
import org.apache.cockpit.connectors.api.jdbc.converter.AbstractJdbcRowConverter;
import org.apache.cockpit.connectors.api.type.ArrayType;
import org.apache.cockpit.connectors.api.type.SeaTunnelDataType;
import org.apache.cockpit.connectors.api.util.JdbcFieldTypeUtils;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;

/**
 * Hive JDBC 行转换器
 * 专门用于处理Hive 3.x的数据类型转换
 */
@Slf4j
public class HiveJdbcRowConverter extends AbstractJdbcRowConverter {

    private final HiveTypeConverter typeConverter;

    public HiveJdbcRowConverter() {
        this.typeConverter = new HiveTypeConverter();
    }

    public HiveJdbcRowConverter(HiveTypeConverter typeConverter) {
        this.typeConverter = typeConverter;
    }

    @Override
    public String converterName() {
        return "HiveJdbcRowConverter";
    }

    @Override
    protected LocalTime readTime(ResultSet rs, int resultSetIndex) throws SQLException {
        // Hive的TIME类型实际上存储为字符串或时间戳
        // 尝试从字符串或时间戳中解析时间
        try {
            String timeStr = rs.getString(resultSetIndex);
            if (timeStr != null && !timeStr.isEmpty()) {
                // 尝试解析HH:MM:SS格式
                return LocalTime.parse(timeStr);
            }
        } catch (Exception e) {
            // 如果字符串解析失败，尝试使用标准的时间解析
            log.debug("Failed to parse time as string, trying standard time parsing", e);
        }

        // 使用父类的标准时间解析
        Time sqlTime = JdbcFieldTypeUtils.getTime(rs, resultSetIndex);
        return Optional.ofNullable(sqlTime).map(Time::toLocalTime).orElse(null);
    }

    @Override
    public Object[] convertToArray(
            ResultSet rs,
            int resultSetIndex,
            SeaTunnelDataType<?> seaTunnelDataType,
            String fieldName)
            throws SQLException {
        // Hive的数组类型通常以字符串形式存储，例如 "[1,2,3]"
        try {
            String arrayString = rs.getString(resultSetIndex);
            if (arrayString == null || arrayString.trim().isEmpty()) {
                return null;
            }

            // 移除方括号并分割
            arrayString = arrayString.trim();
            if (arrayString.startsWith("[") && arrayString.endsWith("]")) {
                arrayString = arrayString.substring(1, arrayString.length() - 1);
            }

            String[] stringElements = arrayString.split(",");
            SeaTunnelDataType<?> elementType = ((ArrayType<?, ?>) seaTunnelDataType).getElementType();

            switch (elementType.getSqlType()) {
                case STRING:
                    return stringElements;
                case BOOLEAN:
                    Boolean[] booleans = new Boolean[stringElements.length];
                    for (int i = 0; i < stringElements.length; i++) {
                        booleans[i] = Boolean.parseBoolean(stringElements[i].trim());
                    }
                    return booleans;
                case TINYINT:
                    Byte[] bytes = new Byte[stringElements.length];
                    for (int i = 0; i < stringElements.length; i++) {
                        bytes[i] = Byte.parseByte(stringElements[i].trim());
                    }
                    return bytes;
                case SMALLINT:
                    Short[] shorts = new Short[stringElements.length];
                    for (int i = 0; i < stringElements.length; i++) {
                        shorts[i] = Short.parseShort(stringElements[i].trim());
                    }
                    return shorts;
                case INT:
                    Integer[] integers = new Integer[stringElements.length];
                    for (int i = 0; i < stringElements.length; i++) {
                        integers[i] = Integer.parseInt(stringElements[i].trim());
                    }
                    return integers;
                case BIGINT:
                    Long[] longs = new Long[stringElements.length];
                    for (int i = 0; i < stringElements.length; i++) {
                        longs[i] = Long.parseLong(stringElements[i].trim());
                    }
                    return longs;
                case FLOAT:
                    Float[] floats = new Float[stringElements.length];
                    for (int i = 0; i < stringElements.length; i++) {
                        floats[i] = Float.parseFloat(stringElements[i].trim());
                    }
                    return floats;
                case DOUBLE:
                    Double[] doubles = new Double[stringElements.length];
                    for (int i = 0; i < stringElements.length; i++) {
                        doubles[i] = Double.parseDouble(stringElements[i].trim());
                    }
                    return doubles;
                case DECIMAL:
                    BigDecimal[] decimals = new BigDecimal[stringElements.length];
                    for (int i = 0; i < stringElements.length; i++) {
                        decimals[i] = new BigDecimal(stringElements[i].trim());
                    }
                    return decimals;
                default:
                    String type = String.format("Array[%s]", elementType.getSqlType());
                    throw CommonError.unsupportedDataType(converterName(), type, fieldName);
            }
        } catch (SQLException e) {
            // 如果字符串解析失败，尝试使用标准的数组解析
            log.debug("Failed to parse array as string, trying standard array parsing", e);
            return super.convertToArray(rs, resultSetIndex, seaTunnelDataType, fieldName);
        }
    }

    @Override
    protected void writeTime(PreparedStatement statement, int index, LocalTime time)
            throws SQLException {
        // Hive可能需要将时间存储为字符串格式
        try {
            statement.setTime(index, Time.valueOf(time));
        } catch (SQLException e) {
            // 如果标准时间设置失败，尝试设置为字符串格式
            log.debug("Failed to set time using Time.valueOf, trying string format", e);
            statement.setString(index, time.toString());
        }
    }

    @Override
    protected void setValueToStatementByDataType(
            Object value,
            PreparedStatement statement,
            SeaTunnelDataType<?> seaTunnelDataType,
            int statementIndex,
            @Nullable String sourceType)
            throws SQLException {

        // Hive特定的类型处理
        if (sourceType != null && sourceType.toUpperCase().contains("ARRAY")) {
            handleArrayType(value, statement, seaTunnelDataType, statementIndex, sourceType);
            return;
        }

        if (sourceType != null && sourceType.toUpperCase().contains("MAP")) {
            handleMapType(value, statement, statementIndex);
            return;
        }

        if (sourceType != null && sourceType.toUpperCase().contains("STRUCT")) {
            handleStructType(value, statement, statementIndex);
            return;
        }

        // 处理标准数据类型
        super.setValueToStatementByDataType(value, statement, seaTunnelDataType, statementIndex, sourceType);
    }

    /**
     * 处理Hive数组类型
     */
    private void handleArrayType(
            Object value,
            PreparedStatement statement,
            SeaTunnelDataType<?> seaTunnelDataType,
            int statementIndex,
            String sourceType)
            throws SQLException {

        if (value == null) {
            statement.setNull(statementIndex, Types.VARCHAR); // Hive数组通常存储为字符串
            return;
        }

        if (value instanceof Object[]) {
            Object[] array = (Object[]) value;
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < array.length; i++) {
                if (i > 0) {
                    sb.append(",");
                }
                if (array[i] != null) {
                    sb.append(array[i].toString());
                } else {
                    sb.append("null");
                }
            }
            sb.append("]");
            statement.setString(statementIndex, sb.toString());
        } else {
            // 如果不是数组，直接设置为字符串
            statement.setString(statementIndex, value.toString());
        }
    }

    /**
     * 处理Hive映射类型
     */
    private void handleMapType(Object value, PreparedStatement statement, int statementIndex)
            throws SQLException {

        if (value == null) {
            statement.setNull(statementIndex, Types.VARCHAR); // Hive映射通常存储为字符串
            return;
        }

        // 将映射转换为字符串表示，例如 "{key1:value1,key2:value2}"
        statement.setString(statementIndex, value.toString());
    }

    /**
     * 处理Hive结构体类型
     */
    private void handleStructType(Object value, PreparedStatement statement, int statementIndex)
            throws SQLException {

        if (value == null) {
            statement.setNull(statementIndex, Types.VARCHAR); // Hive结构体通常存储为字符串
            return;
        }

        // 将结构体转换为字符串表示
        statement.setString(statementIndex, value.toString());
    }

    /**
     * 将SeaTunnel数据类型转换为Hive JDBC类型
     */
    public int toHiveJdbcType(SeaTunnelDataType<?> seaTunnelDataType) {
        switch (seaTunnelDataType.getSqlType()) {
            case BOOLEAN:
                return Types.BOOLEAN;
            case TINYINT:
                return Types.TINYINT;
            case SMALLINT:
                return Types.SMALLINT;
            case INT:
                return Types.INTEGER;
            case BIGINT:
                return Types.BIGINT;
            case FLOAT:
                return Types.FLOAT;
            case DOUBLE:
                return Types.DOUBLE;
            case DECIMAL:
                return Types.DECIMAL;
            case STRING:
                return Types.VARCHAR;
            case DATE:
                return Types.DATE;
            case TIME:
                return Types.TIME;
            case TIMESTAMP:
                return Types.TIMESTAMP;
            case BYTES:
                return Types.BINARY;
            case ARRAY:
                return Types.ARRAY;
            case NULL:
                return Types.NULL;
            default:
                // 对于MAP、ROW等复杂类型，使用VARCHAR
                return Types.VARCHAR;
        }
    }

    /**
     * 获取Hive类型转换器
     */
    public HiveTypeConverter getTypeConverter() {
        return typeConverter;
    }

    /**
     * 检查值是否与Hive类型兼容
     */
    public boolean isCompatibleWithHiveType(Object value, String hiveType) {
        if (value == null) {
            return true; // NULL值总是兼容的
        }

        try {
            HiveType type = HiveType.getByName(hiveType);

            switch (type) {
                case BOOLEAN:
                    return value instanceof Boolean;
                case TINYINT:
                    return value instanceof Byte;
                case SMALLINT:
                    return value instanceof Short;
                case INT:
                case INTEGER:
                    return value instanceof Integer;
                case BIGINT:
                    return value instanceof Long;
                case FLOAT:
                    return value instanceof Float;
                case DOUBLE:
                    return value instanceof Double;
                case DECIMAL:
                    return value instanceof BigDecimal;
                case STRING:
                case VARCHAR:
                case CHAR:
                    return value instanceof String;
                case DATE:
                    return value instanceof LocalDate;
                case TIMESTAMP:
                    return value instanceof LocalDateTime;
                case BINARY:
                    return value instanceof byte[];
                case ARRAY:
                    return value instanceof Object[];
                default:
                    return true; // 对于复杂类型，假设总是兼容
            }
        } catch (Exception e) {
            log.warn("Failed to check compatibility with Hive type: {}", hiveType, e);
            return false;
        }
    }
}