package org.apache.cockpit.connectors.api.source.converter;

import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.arrow.vector.types.Types;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class MapConverter implements Converter<MapVector> {
    @Override
    public Object convert(int rowIndex, MapVector fieldVector) {
        return fieldVector.isNull(rowIndex) ? null : fieldVector.getObject(rowIndex);
    }

    @Override
    public Object convert(
            int rowIndex, MapVector fieldVector, Map<String, Function> genericsConverters) {
        UnionMapReader reader = fieldVector.getReader();
        reader.setPosition(rowIndex);
        Map<Object, Object> mapValue = new HashMap<>();
        Function keyConverter = genericsConverters.get(MAP_KEY);
        Function valueConverter = genericsConverters.get(MAP_VALUE);
        while (reader.next()) {
            Object key = keyConverter.apply(processTimeZone(reader.key().readObject()));
            Object value = valueConverter.apply(processTimeZone(reader.value().readObject()));
            mapValue.put(key, value);
        }
        return mapValue;
    }

    private Object processTimeZone(Object value) {
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value)
                    .atZone(ZoneOffset.UTC)
                    .withZoneSameInstant(ZoneId.systemDefault())
                    .toLocalDateTime();
        } else {
            return value;
        }
    }

    @Override
    public boolean support(Types.MinorType type) {
        return Types.MinorType.MAP == type;
    }
}
