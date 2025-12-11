package org.apache.cockpit.connectors.api.util;

public class TypeDefineUtils {
    public static Long charToDoubleByteLength(Long charLength) {
        if (charLength == null) {
            return null;
        }
        return charLength * 2;
    }

    public static Long doubleByteTo4ByteLength(Long doubleByteLength) {
        if (doubleByteLength == null) {
            return null;
        }
        return doubleByteLength * 2;
    }

    public static Long charTo4ByteLength(Long charLength) {
        return charToByteLength(charLength, 4);
    }

    public static Long charToByteLength(Long charLength, int byteSize) {
        if (charLength == null) {
            return null;
        }
        return charLength * byteSize;
    }
}
