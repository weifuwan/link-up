package org.apache.cockpit.connectors.influxdb.serialize;

import com.google.common.base.Strings;
import org.apache.cockpit.connectors.api.catalog.exception.CommonErrorCodeDeprecated;
import org.apache.cockpit.connectors.api.type.SeaTunnelDataType;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.influxdb.exception.InfluxdbConnectorException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.influxdb.dto.Point;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DefaultSerializer implements Serializer {
    private final SeaTunnelRowType seaTunnelRowType;

    private final BiConsumer<SeaTunnelRow, Point.Builder> timestampExtractor;
    private final BiConsumer<SeaTunnelRow, Point.Builder> fieldExtractor;
    private final BiConsumer<SeaTunnelRow, Point.Builder> tagExtractor;
    private final String measurement;

    private final TimeUnit precision;

    public DefaultSerializer(
            SeaTunnelRowType seaTunnelRowType,
            TimeUnit precision,
            List<String> tagKeys,
            String timestampKey,
            String measurement) {
        this.measurement = measurement;
        this.seaTunnelRowType = seaTunnelRowType;
        this.timestampExtractor = createTimestampExtractor(seaTunnelRowType, timestampKey);
        this.tagExtractor = createTagExtractor(seaTunnelRowType, tagKeys);
        List<String> fieldKeys = getFieldKeys(seaTunnelRowType, timestampKey, tagKeys);
        this.fieldExtractor = createFieldExtractor(seaTunnelRowType, fieldKeys);
        this.precision = precision;
    }

    @Override
    public Point serialize(SeaTunnelRow seaTunnelRow) {
        Point.Builder builder = Point.measurement(measurement);
        timestampExtractor.accept(seaTunnelRow, builder);
        tagExtractor.accept(seaTunnelRow, builder);
        fieldExtractor.accept(seaTunnelRow, builder);
        return builder.build();
    }

    private BiConsumer<SeaTunnelRow, Point.Builder> createFieldExtractor(
            SeaTunnelRowType seaTunnelRowType, List<String> fieldKeys) {
        return (row, builder) -> {
            for (String field : fieldKeys) {
                int indexOfSeaTunnelRow = seaTunnelRowType.indexOf(field);
                SeaTunnelDataType dataType = seaTunnelRowType.getFieldType(indexOfSeaTunnelRow);
                Object val = row.getField(indexOfSeaTunnelRow);
                switch (dataType.getSqlType()) {
                    case BOOLEAN:
                        builder.addField(field, Boolean.valueOf((Boolean) val));
                        break;
                    case SMALLINT:
                        builder.addField(field, Short.valueOf((Short) val));
                        break;
                    case INT:
                        builder.addField(field, ((Number) val).intValue());
                        break;
                    case BIGINT:
                        // Only timstamp support be bigint,however it is processed in specicalField
                        builder.addField(field, ((Number) val).longValue());
                        break;
                    case FLOAT:
                        builder.addField(field, ((Number) val).floatValue());
                        break;
                    case DOUBLE:
                        builder.addField(field, ((Number) val).doubleValue());
                        break;
                    case STRING:
                        builder.addField(field, val.toString());
                        break;
                    default:
                        throw new InfluxdbConnectorException(
                                CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                                "Unsupported data type: " + dataType);
                }
            }
        };
    }

    private BiConsumer<SeaTunnelRow, Point.Builder> createTimestampExtractor(
            SeaTunnelRowType seaTunnelRowType, String timeKey) {
        // not config timeKey, use processing time
        if (Strings.isNullOrEmpty(timeKey)) {
            return (row, builder) -> builder.time(System.currentTimeMillis(), precision);
        }

        int timeFieldIndex = seaTunnelRowType.indexOf(timeKey);
        return (row, builder) -> {
            Object time = row.getField(timeFieldIndex);
            if (time == null) {
                builder.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
            }
            SeaTunnelDataType<?> timestampFieldType = seaTunnelRowType.getFieldType(timeFieldIndex);
            switch (timestampFieldType.getSqlType()) {
                case STRING:
                    builder.time(Long.parseLong((String) time), precision);
                    break;
                case TIMESTAMP:
                    builder.time(
                            ((LocalDateTime) time)
                                    .atZone(ZoneOffset.UTC)
                                    .toInstant()
                                    .toEpochMilli(),
                            precision);
                    break;
                case BIGINT:
                    builder.time((Long) time, precision);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported data type: " + timestampFieldType);
            }
        };
    }

    private BiConsumer<SeaTunnelRow, Point.Builder> createTagExtractor(
            SeaTunnelRowType seaTunnelRowType, List<String> tagKeys) {
        // not config tagKeys
        if (CollectionUtils.isEmpty(tagKeys)) {
            return (row, builder) -> {};
        }

        return (row, builder) -> {
            for (String tagKey : tagKeys) {
                int indexOfSeaTunnelRow = seaTunnelRowType.indexOf(tagKey);
                builder.tag(tagKey, row.getField(indexOfSeaTunnelRow).toString());
            }
        };
    }

    private List<String> getFieldKeys(
            SeaTunnelRowType seaTunnelRowType, String timestampKey, List<String> tagKeys) {
        return Stream.of(seaTunnelRowType.getFieldNames())
                .filter(name -> CollectionUtils.isEmpty(tagKeys) || !tagKeys.contains(name))
                .filter(name -> StringUtils.isEmpty(timestampKey) || !name.equals(timestampKey))
                .collect(Collectors.toList());
    }
}
