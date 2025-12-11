package org.apache.cockpit.connectors.psql.dialect;



import org.apache.cockpit.connectors.api.catalog.Column;
import org.apache.cockpit.connectors.api.catalog.TableSchema;
import org.apache.cockpit.connectors.api.catalog.exception.CommonErrorCodeDeprecated;
import org.apache.cockpit.connectors.api.jdbc.converter.AbstractJdbcRowConverter;
import org.apache.cockpit.connectors.api.jdbc.dialect.DatabaseIdentifier;
import org.apache.cockpit.connectors.api.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.cockpit.connectors.api.jdbc.exception.JdbcConnectorException;
import org.apache.cockpit.connectors.api.type.*;
import org.apache.cockpit.connectors.api.util.JdbcFieldTypeUtils;
import org.postgresql.util.PGobject;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Locale;
import java.util.Optional;

import static org.apache.cockpit.connectors.psql.dialect.PostgresTypeConverter.PG_INET;


public class PostgresJdbcRowConverter extends AbstractJdbcRowConverter {

    private static final String PG_GEOMETRY = "GEOMETRY";
    private static final String PG_GEOGRAPHY = "GEOGRAPHY";

    @Override
    public String converterName() {
        return DatabaseIdentifier.POSTGRESQL;
    }

    @Override
    public SeaTunnelRow toInternal(ResultSet rs, TableSchema tableSchema) throws SQLException {
        SeaTunnelRowType typeInfo = tableSchema.toPhysicalRowDataType();
        Object[] fields = new Object[typeInfo.getTotalFields()];
        for (int fieldIndex = 0; fieldIndex < typeInfo.getTotalFields(); fieldIndex++) {
            SeaTunnelDataType<?> seaTunnelDataType = typeInfo.getFieldType(fieldIndex);
            int resultSetIndex = fieldIndex + 1;
            String metaDataColumnType =
                    rs.getMetaData().getColumnTypeName(resultSetIndex).toUpperCase(Locale.ROOT);
            switch (seaTunnelDataType.getSqlType()) {
                case STRING:
                    if (metaDataColumnType.equals(PG_GEOMETRY)
                            || metaDataColumnType.equals(PG_GEOGRAPHY)) {
                        fields[fieldIndex] =
                                rs.getObject(resultSetIndex) == null
                                        ? null
                                        : rs.getObject(resultSetIndex).toString();
                    } else {
                        fields[fieldIndex] = JdbcFieldTypeUtils.getString(rs, resultSetIndex);
                    }
                    break;
                case BOOLEAN:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getBoolean(rs, resultSetIndex);
                    break;
                case TINYINT:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getByte(rs, resultSetIndex);
                    break;
                case SMALLINT:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getShort(rs, resultSetIndex);
                    break;
                case INT:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getInt(rs, resultSetIndex);
                    break;
                case BIGINT:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getLong(rs, resultSetIndex);
                    break;
                case FLOAT:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getFloat(rs, resultSetIndex);
                    break;
                case DOUBLE:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getDouble(rs, resultSetIndex);
                    break;
                case DECIMAL:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getBigDecimal(rs, resultSetIndex);
                    break;
                case DATE:
                    Date sqlDate = JdbcFieldTypeUtils.getDate(rs, resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlDate).map(e -> e.toLocalDate()).orElse(null);
                    break;
                case TIME:
                    Time sqlTime = JdbcFieldTypeUtils.getTime(rs, resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlTime).map(e -> e.toLocalTime()).orElse(null);
                    break;
                case TIMESTAMP:
                    Timestamp sqlTimestamp = JdbcFieldTypeUtils.getTimestamp(rs, resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlTimestamp)
                                    .map(e -> e.toLocalDateTime())
                                    .orElse(null);
                    break;
                case BYTES:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getBytes(rs, resultSetIndex);
                    break;
                case NULL:
                    fields[fieldIndex] = null;
                    break;
                case ARRAY:
                    Array jdbcArray = rs.getArray(resultSetIndex);
                    if (jdbcArray == null) {
                        fields[fieldIndex] = null;
                        break;
                    }

                    Object arrayObject = jdbcArray.getArray();
                    if (((ArrayType) seaTunnelDataType)
                            .getTypeClass()
                            .equals(arrayObject.getClass())) {
                        fields[fieldIndex] = arrayObject;
                    } else {
                        throw new JdbcConnectorException(
                                CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                                "Unexpected value: " + seaTunnelDataType.getTypeClass());
                    }
                    break;
                case MAP:
                case ROW:
                default:
                    throw new JdbcConnectorException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                            "Unexpected value: " + seaTunnelDataType);
            }
        }
        return new SeaTunnelRow(fields);
    }

    @Override
    public PreparedStatement toExternal(
            TableSchema tableSchema,
            @Nullable TableSchema databaseTableSchema,
            SeaTunnelRow row,
            PreparedStatement statement)
            throws SQLException {
        SeaTunnelRowType rowType = tableSchema.toPhysicalRowDataType();
        String[] sourceTypes =
                tableSchema.getColumns().stream()
                        .filter(Column::isPhysical)
                        .map(Column::getSourceType)
                        .toArray(String[]::new);
        for (int fieldIndex = 0; fieldIndex < rowType.getTotalFields(); fieldIndex++) {
            try {
                SeaTunnelDataType<?> seaTunnelDataType = rowType.getFieldType(fieldIndex);
                int statementIndex = fieldIndex + 1;
                Object fieldValue = row.getField(fieldIndex);
                if (fieldValue == null) {
                    statement.setObject(statementIndex, null);
                    continue;
                }

                switch (seaTunnelDataType.getSqlType()) {
                    case STRING:
                        String sourceType = sourceTypes[fieldIndex];
                        if (PG_INET.equalsIgnoreCase(sourceType)) {
                            PGobject inetObject = new PGobject();
                            inetObject.setType(PG_INET);
                            inetObject.setValue(String.valueOf(row.getField(fieldIndex)));
                            statement.setObject(statementIndex, inetObject);
                        } else {
                            statement.setString(statementIndex, (String) row.getField(fieldIndex));
                        }
                        break;
                    case BOOLEAN:
                        statement.setBoolean(statementIndex, (Boolean) row.getField(fieldIndex));
                        break;
                    case TINYINT:
                        statement.setByte(statementIndex, (Byte) row.getField(fieldIndex));
                        break;
                    case SMALLINT:
                        statement.setShort(statementIndex, (Short) row.getField(fieldIndex));
                        break;
                    case INT:
                        statement.setInt(statementIndex, (Integer) row.getField(fieldIndex));
                        break;
                    case BIGINT:
                        statement.setLong(statementIndex, (Long) row.getField(fieldIndex));
                        break;
                    case FLOAT:
                        statement.setFloat(statementIndex, (Float) row.getField(fieldIndex));
                        break;
                    case DOUBLE:
                        statement.setDouble(statementIndex, (Double) row.getField(fieldIndex));
                        break;
                    case DECIMAL:
                        statement.setBigDecimal(
                                statementIndex, (BigDecimal) row.getField(fieldIndex));
                        break;
                    case DATE:
                        LocalDate localDate = (LocalDate) row.getField(fieldIndex);
                        statement.setDate(statementIndex, Date.valueOf(localDate));
                        break;
                    case TIME:
                        writeTime(statement, statementIndex, (LocalTime) row.getField(fieldIndex));
                        break;
                    case TIMESTAMP:
                        LocalDateTime localDateTime = (LocalDateTime) row.getField(fieldIndex);
                        statement.setTimestamp(
                                statementIndex, Timestamp.valueOf(localDateTime));
                        break;
                    case BYTES:
                        statement.setBytes(statementIndex, (byte[]) row.getField(fieldIndex));
                        break;
                    case NULL:
                        statement.setNull(statementIndex, java.sql.Types.NULL);
                        break;
                    case ARRAY:
                        SeaTunnelDataType elementType =
                                ((ArrayType) seaTunnelDataType).getElementType();
                        Object[] array = (Object[]) row.getField(fieldIndex);
                        if (array == null) {
                            statement.setNull(statementIndex, java.sql.Types.ARRAY);
                            break;
                        }
                        if (SqlType.TINYINT.equals(elementType.getSqlType())) {
                            Short[] shortArray = new Short[array.length];
                            for (int i = 0; i < array.length; i++) {
                                shortArray[i] = Short.valueOf(array[i].toString());
                            }
                            statement.setObject(statementIndex, shortArray);
                        } else {
                            statement.setObject(statementIndex, array);
                        }
                        break;
                    case MAP:
                    case ROW:
                    default:
                        throw new JdbcConnectorException(
                                CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                                "Unexpected value: " + seaTunnelDataType);
                }
            } catch (Exception e) {
                throw new JdbcConnectorException(
                        JdbcConnectorErrorCode.DATA_TYPE_CAST_FAILED,
                        "error field:" + rowType.getFieldNames()[fieldIndex],
                        e);
            }
        }
        return statement;
    }
}
