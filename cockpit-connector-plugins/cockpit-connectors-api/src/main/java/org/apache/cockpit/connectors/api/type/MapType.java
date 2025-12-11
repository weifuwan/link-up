package org.apache.cockpit.connectors.api.type;


import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class MapType<K, V> implements CompositeType<Map<K, V>> {

    private static final List<SqlType> SUPPORTED_KEY_TYPES =
            Arrays.asList(
                    SqlType.NULL,
                    SqlType.BOOLEAN,
                    SqlType.TINYINT,
                    SqlType.SMALLINT,
                    SqlType.INT,
                    SqlType.BIGINT,
                    SqlType.DATE,
                    SqlType.TIME,
                    SqlType.TIMESTAMP,
                    SqlType.TIMESTAMP_TZ,
                    SqlType.FLOAT,
                    SqlType.DOUBLE,
                    SqlType.STRING,
                    SqlType.DECIMAL);

    private final SeaTunnelDataType<K> keyType;
    private final SeaTunnelDataType<V> valueType;

    public MapType(SeaTunnelDataType<K> keyType, SeaTunnelDataType<V> valueType) {
        checkNotNull(keyType, "The key type is required.");
        checkNotNull(valueType, "The value type is required.");
        checkArgument(
                SUPPORTED_KEY_TYPES.contains(keyType.getSqlType()),
                "Unsupported key types: %s",
                keyType);
        this.keyType = keyType;
        this.valueType = valueType;
    }

    public SeaTunnelDataType<K> getKeyType() {
        return keyType;
    }

    public SeaTunnelDataType<V> getValueType() {
        return valueType;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<Map<K, V>> getTypeClass() {
        return (Class<Map<K, V>>) (Class<?>) Map.class;
    }

    @Override
    public SqlType getSqlType() {
        return SqlType.MAP;
    }

    @Override
    public List<SeaTunnelDataType<?>> getChildren() {
        return Lists.newArrayList(this.keyType, this.valueType);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof MapType)) {
            return false;
        }
        MapType<?, ?> that = (MapType<?, ?>) obj;
        return Objects.equals(keyType, that.keyType) && Objects.equals(valueType, that.valueType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyType, valueType);
    }

    @Override
    public String toString() {
        return String.format("Map<%s, %s>", keyType, valueType);
    }
}
