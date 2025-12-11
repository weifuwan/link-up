package org.apache.cockpit.connectors.api.jdbc.converter;

public interface BasicDataTypeConverter<T extends BasicTypeDefine>
        extends BasicTypeConverter<T>, BasicDataConverter<T> {}
