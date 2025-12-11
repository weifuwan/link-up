package org.apache.cockpit.connectors.api.source.converter;

import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.types.Types;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class FixedSizeListConverter implements Converter<FixedSizeListVector> {
    @Override
    public Object convert(int rowIndex, FixedSizeListVector fieldVector) {
        return fieldVector.isNull(rowIndex) ? null : fieldVector.getObject(rowIndex);
    }

    @Override
    public Object convert(
            int rowIndex,
            FixedSizeListVector fieldVector,
            Map<String, Function> genericsConverters) {
        if (fieldVector.isNull(rowIndex)) {
            return null;
        }
        List<?> listData = fieldVector.getObject(rowIndex);
        Function converter = genericsConverters.get(ARRAY_KEY);
        return listData.stream()
                .map(
                        item -> {
                            if (item instanceof LocalDateTime) {
                                LocalDateTime localDateTime =
                                        ((LocalDateTime) item)
                                                .atZone(ZoneOffset.UTC)
                                                .withZoneSameInstant(ZoneId.systemDefault())
                                                .toLocalDateTime();
                                return converter.apply(localDateTime);
                            } else {
                                return converter.apply(item);
                            }
                        })
                .collect(Collectors.toList());
    }

    @Override
    public boolean support(Types.MinorType type) {
        return Types.MinorType.FIXED_SIZE_LIST == type;
    }
}
