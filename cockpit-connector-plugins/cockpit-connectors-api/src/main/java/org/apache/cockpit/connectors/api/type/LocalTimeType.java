package org.apache.cockpit.connectors.api.type;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.temporal.Temporal;
import java.util.Objects;

public class LocalTimeType<T extends Temporal> implements SeaTunnelDataType<T> {
    private static final long serialVersionUID = 2L;

    public static final LocalTimeType<LocalDate> LOCAL_DATE_TYPE =
            new LocalTimeType<>(LocalDate.class, SqlType.DATE);
    public static final LocalTimeType<LocalTime> LOCAL_TIME_TYPE =
            new LocalTimeType<>(LocalTime.class, SqlType.TIME);
    public static final LocalTimeType<LocalDateTime> LOCAL_DATE_TIME_TYPE =
            new LocalTimeType<>(LocalDateTime.class, SqlType.TIMESTAMP);
    public static final LocalTimeType<OffsetDateTime> OFFSET_DATE_TIME_TYPE =
            new LocalTimeType<>(OffsetDateTime.class, SqlType.TIMESTAMP_TZ);

    private final Class<T> typeClass;
    private final SqlType sqlType;

    private LocalTimeType(Class<T> typeClass, SqlType sqlType) {
        this.typeClass = typeClass;
        this.sqlType = sqlType;
    }

    @Override
    public Class<T> getTypeClass() {
        return typeClass;
    }

    @Override
    public SqlType getSqlType() {
        return this.sqlType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeClass);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof LocalTimeType)) {
            return false;
        }
        LocalTimeType<?> that = (LocalTimeType<?>) obj;
        return Objects.equals(typeClass, that.typeClass);
    }

    @Override
    public String toString() {
        return sqlType.toString();
    }
}
