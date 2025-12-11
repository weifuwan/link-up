package org.apache.cockpit.connectors.api.source.converter;

import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.types.Types;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

public class DateMilliConvertor implements Converter<DateMilliVector> {
    @Override
    public Object convert(int rowIndex, DateMilliVector fieldVector) {
        if (fieldVector == null || fieldVector.isNull(rowIndex)) {
            return null;
        }
        LocalDateTime localDateTime = fieldVector.getObject(rowIndex);
        return localDateTime
                .atZone(ZoneOffset.UTC)
                .withZoneSameInstant(ZoneId.systemDefault())
                .toLocalDateTime();
    }

    @Override
    public boolean support(Types.MinorType type) {
        return Types.MinorType.DATEMILLI == type;
    }
}
