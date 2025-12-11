
package org.apache.cockpit.connectors.api.source.converter;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.Types;

public class DefaultConverter implements Converter<FieldVector> {

    @Override
    public Object convert(int rowIndex, FieldVector fieldVector) {
        return fieldVector.isNull(rowIndex) ? null : fieldVector.getObject(rowIndex);
    }

    @Override
    public boolean support(Types.MinorType type) {
        return false;
    }
}
