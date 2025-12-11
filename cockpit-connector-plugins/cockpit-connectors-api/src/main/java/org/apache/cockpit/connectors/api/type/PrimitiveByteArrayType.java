package org.apache.cockpit.connectors.api.type;

public class PrimitiveByteArrayType implements SeaTunnelDataType<byte[]> {
    public static final PrimitiveByteArrayType INSTANCE = new PrimitiveByteArrayType();

    private PrimitiveByteArrayType() {}

    @Override
    public Class<byte[]> getTypeClass() {
        return byte[].class;
    }

    @Override
    public SqlType getSqlType() {
        return SqlType.BYTES;
    }

    @Override
    public int hashCode() {
        return byte[].class.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        return obj instanceof PrimitiveByteArrayType;
    }

    @Override
    public String toString() {
        return SqlType.BYTES.toString();
    }
}
