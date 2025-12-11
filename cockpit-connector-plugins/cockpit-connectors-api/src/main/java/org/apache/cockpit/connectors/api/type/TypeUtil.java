package org.apache.cockpit.connectors.api.type;

public class TypeUtil {

    /** Check if the data type can be converted to another data type. */
    public static boolean canConvert(SeaTunnelDataType<?> from, SeaTunnelDataType<?> to) {
        // any type can be converted to string
        if (from == to || to.getSqlType() == SqlType.STRING) {
            return true;
        }
        if (from.getSqlType() == SqlType.TINYINT) {
            return to.getSqlType() == SqlType.SMALLINT
                    || to.getSqlType() == SqlType.INT
                    || to.getSqlType() == SqlType.BIGINT;
        }
        if (from.getSqlType() == SqlType.SMALLINT) {
            return to.getSqlType() == SqlType.INT || to.getSqlType() == SqlType.BIGINT;
        }
        if (from.getSqlType() == SqlType.INT) {
            return to.getSqlType() == SqlType.BIGINT;
        }
        if (from.getSqlType() == SqlType.FLOAT) {
            return to.getSqlType() == SqlType.DOUBLE;
        }
        return false;
    }
}
