package org.apache.cockpit.connectors.api.source.converter;

import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.types.Types;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

public class TimeStampSecConverter implements Converter<TimeStampSecVector> {
    @Override
    public Object convert(int rowIndex, TimeStampSecVector fieldVector) {
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
        return Types.MinorType.TIMESTAMPSEC == type;
    }
}
