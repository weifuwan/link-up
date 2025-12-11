package org.apache.cockpit.connectors.api.jdbc.dialect;


import org.apache.cockpit.connectors.api.catalog.Column;
import org.apache.cockpit.connectors.api.jdbc.converter.BasicTypeDefine;

public class GenericTypeMapper implements JdbcDialectTypeMapper {

    private GenericTypeConverter typeConverter;

    public GenericTypeMapper() {
        this(GenericTypeConverter.DEFAULT_INSTANCE);
    }

    public GenericTypeMapper(GenericTypeConverter typeConverter) {
        this.typeConverter = typeConverter;
    }

    @Override
    public Column mappingColumn(BasicTypeDefine typeDefine) {
        return typeConverter.convert(typeDefine);
    }
}
