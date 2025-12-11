package org.apache.cockpit.connectors.api.serialization;



import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.cockpit.connectors.api.catalog.exception.CommonErrorCodeDeprecated;
import org.apache.cockpit.connectors.api.type.*;
import org.apache.cockpit.connectors.api.util.SeaTunnelJsonFormatException;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntFunction;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;

public class RowToJsonConverters implements Serializable {

    private static final long serialVersionUID = 6988876688930916940L;

    private String nullValue;

    public RowToJsonConverter createConverter(SeaTunnelDataType<?> type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    public RowToJsonConverter createConverter(SeaTunnelDataType<?> type, String nullValue) {
        this.nullValue = nullValue;
        return createConverter(type);
    }

    private RowToJsonConverter wrapIntoNullableConverter(RowToJsonConverter converter) {
        return new RowToJsonConverter() {
            @Override
            public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                if (value == null) {
                    if (nullValue != null) {
                        return mapper.getNodeFactory().textNode(nullValue);
                    }
                    return mapper.getNodeFactory().nullNode();
                }
                return converter.convert(mapper, reuse, value);
            }
        };
    }

    private RowToJsonConverter createNotNullConverter(SeaTunnelDataType<?> type) {
        SqlType sqlType = type.getSqlType();
        switch (sqlType) {
            case ROW:
                return createRowConverter((SeaTunnelRowType) type);
            case NULL:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return nullValue == null
                                ? null
                                : mapper.getNodeFactory().textNode((String) value);
                    }
                };
            case BOOLEAN:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().booleanNode((Boolean) value);
                    }
                };
            case TINYINT:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().numberNode((byte) value);
                    }
                };
            case SMALLINT:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().numberNode((short) value);
                    }
                };
            case INT:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().numberNode((int) value);
                    }
                };
            case BIGINT:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().numberNode((long) value);
                    }
                };
            case FLOAT:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().numberNode((float) value);
                    }
                };
            case DOUBLE:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().numberNode((double) value);
                    }
                };
            case DECIMAL:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().numberNode((BigDecimal) value);
                    }
                };
            case BYTES:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().binaryNode((byte[]) value);
                    }
                };
            case STRING:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().textNode((String) value);
                    }
                };
            case DATE:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory()
                                .textNode(ISO_LOCAL_DATE.format((LocalDate) value));
                    }
                };
            case TIME:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory()
                                .textNode(TimeFormat.TIME_FORMAT.format((LocalTime) value));
                    }
                };
            case TIMESTAMP:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory()
                                .textNode(ISO_LOCAL_DATE_TIME.format((LocalDateTime) value));
                    }
                };
            case ARRAY:
                return createArrayConverter((ArrayType) type);
            case MAP:
                MapType mapType = (MapType) type;
                return createMapConverter(mapType.getKeyType(), mapType.getValueType());
            default:
                throw new SeaTunnelJsonFormatException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        "unsupported parse type: " + type);
        }
    }

    private RowToJsonConverter createRowConverter(SeaTunnelRowType rowType) {
        final RowToJsonConverter[] fieldConverters =
                Arrays.stream(rowType.getFieldTypes())
                        .map(
                                new Function<SeaTunnelDataType<?>, Object>() {
                                    @Override
                                    public Object apply(SeaTunnelDataType<?> seaTunnelDataType) {
                                        return createConverter(seaTunnelDataType);
                                    }
                                })
                        .toArray(
                                new IntFunction<RowToJsonConverter[]>() {
                                    @Override
                                    public RowToJsonConverter[] apply(int value) {
                                        return new RowToJsonConverter[value];
                                    }
                                });
        final String[] fieldNames = rowType.getFieldNames();
        final int arity = fieldNames.length;

        return new RowToJsonConverter() {
            @Override
            public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                ObjectNode node;

                // reuse could be a NullNode if last record is null.
                if (reuse == null || reuse.isNull()) {
                    node = mapper.createObjectNode();
                } else {
                    node = (ObjectNode) reuse;
                }

                for (int i = 0; i < arity; i++) {
                    String fieldName = fieldNames[i];
                    SeaTunnelRow row = (SeaTunnelRow) value;
                    node.set(
                            fieldName,
                            fieldConverters[i].convert(
                                    mapper, node.get(fieldName), row.getField(i)));
                }

                return node;
            }
        };
    }

    private RowToJsonConverter createArrayConverter(ArrayType arrayType) {
        final RowToJsonConverter elementConverter = createConverter(arrayType.getElementType());
        return new RowToJsonConverter() {
            @Override
            public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                ArrayNode node;

                // reuse could be a NullNode if last record is null.
                if (reuse == null || reuse.isNull()) {
                    node = mapper.createArrayNode();
                } else {
                    node = (ArrayNode) reuse;
                    node.removeAll();
                }

                Object[] arrayData = (Object[]) value;
                int numElements = arrayData.length;
                for (int i = 0; i < numElements; i++) {
                    Object element = arrayData[i];
                    node.add(elementConverter.convert(mapper, null, element));
                }

                return node;
            }
        };
    }

    private RowToJsonConverter createMapConverter(
            SeaTunnelDataType<?> keyType, SeaTunnelDataType<?> valueType) {
        final RowToJsonConverter keyConverter = createConverter(keyType);
        final RowToJsonConverter valueConverter = createConverter(valueType);

        return new RowToJsonConverter() {
            @Override
            public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                ObjectNode node;

                // reuse could be a NullNode if last record is null.
                if (reuse == null || reuse.isNull()) {
                    node = mapper.createObjectNode();
                } else {
                    node = (ObjectNode) reuse;
                    node.removeAll();
                }

                Map<?, ?> mapData = (Map) value;
                for (Map.Entry<?, ?> entry : mapData.entrySet()) {
                    // Convert the key to a string using the key converter
                    JsonNode keyNode = keyConverter.convert(mapper, null, entry.getKey());
                    String fieldName = keyNode.isTextual() ? keyNode.asText() : keyNode.toString();

                    node.set(
                            fieldName,
                            valueConverter.convert(mapper, node.get(fieldName), entry.getValue()));
                }

                return node;
            }
        };
    }

    public interface RowToJsonConverter extends Serializable {
        JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value);
    }
}
