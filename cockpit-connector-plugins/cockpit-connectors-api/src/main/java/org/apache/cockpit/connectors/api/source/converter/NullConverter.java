package org.apache.cockpit.connectors.api.source.converter;

import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.types.Types;

public class NullConverter implements Converter<NullVector> {
    @Override
    public Object convert(int rowIndex, NullVector fieldVector) {
        return null;
    }

    @Override
    public boolean support(Types.MinorType type) {
        return Types.MinorType.NULL == type;
    }
}
