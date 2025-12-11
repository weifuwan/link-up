package org.apache.cockpit.connectors.api.type;

public class DecimalArrayType extends ArrayType {
    private static final long serialVersionUID = 1L;

    public static final Class arrayClass = DecimalType[].class;

    public DecimalArrayType(DecimalType elementType) {
        super(arrayClass, elementType);
    }
}
