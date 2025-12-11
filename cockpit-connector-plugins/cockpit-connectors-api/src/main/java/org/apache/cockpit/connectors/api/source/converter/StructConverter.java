package org.apache.cockpit.connectors.api.source.converter;

import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.Types;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class StructConverter implements Converter<StructVector> {
    @Override
    public Object convert(int rowIndex, StructVector fieldVector) {
        return fieldVector.isNull(rowIndex) ? null : fieldVector.getObject(rowIndex);
    }

    @Override
    public Object convert(
            int rowIndex, StructVector fieldVector, Map<String, Function> genericsConverters) {
        Map<String, ?> valueMap = fieldVector.getObject(rowIndex);
        return valueMap.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                e -> {
                                    Optional<Function> optional =
                                            Optional.ofNullable(genericsConverters.get(e.getKey()));
                                    if (optional.isPresent()) {
                                        return optional.get().apply(e.getValue());
                                    } else {
                                        log.warn("No converter found for key:{}", e.getKey());
                                        return e.getValue();
                                    }
                                }));
    }

    @Override
    public boolean support(Types.MinorType type) {
        return Types.MinorType.STRUCT == type;
    }
}
