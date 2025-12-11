package org.apache.cockpit.connectors.api.source.converter;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.Types;

import java.util.Map;
import java.util.function.Function;

public interface Converter<T extends FieldVector> {

    String ARRAY_KEY = "ARRAY";
    String MAP_KEY = "KEY";
    String MAP_VALUE = "VALUE";

    Object convert(int rowIndex, T fieldVector);

    default Object convert(int rowIndex, T fieldVector, Map<String, Function> genericsConverters) {
        throw new UnsupportedOperationException("Unsupported generics convert");
    }

    boolean support(Types.MinorType type);
}
