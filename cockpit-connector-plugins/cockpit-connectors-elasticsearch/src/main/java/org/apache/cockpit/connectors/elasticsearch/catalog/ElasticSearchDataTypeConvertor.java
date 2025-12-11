package org.apache.cockpit.connectors.elasticsearch.catalog;



import com.google.auto.service.AutoService;
import org.apache.cockpit.connectors.api.catalog.Column;
import org.apache.cockpit.connectors.api.catalog.DataTypeConvertor;
import org.apache.cockpit.connectors.api.catalog.PhysicalColumn;
import org.apache.cockpit.connectors.api.jdbc.converter.BasicTypeDefine;
import org.apache.cockpit.connectors.api.type.SeaTunnelDataType;
import org.apache.cockpit.connectors.elasticsearch.client.EsType;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;


/** @deprecated instead by {@link ElasticSearchTypeConverter} */
@Deprecated
@AutoService(DataTypeConvertor.class)
public class ElasticSearchDataTypeConvertor implements DataTypeConvertor<String> {

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String field, String connectorDataType) {
        return toSeaTunnelType(field, connectorDataType, null);
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            String field, String connectorDataType, Map<String, Object> dataTypeProperties) {
        checkNotNull(connectorDataType, "connectorDataType can not be null");
        BasicTypeDefine<EsType> typeDefine =
                BasicTypeDefine.<EsType>builder()
                        .name(field)
                        .columnType(connectorDataType)
                        .dataType(connectorDataType)
                        .build();

        return ElasticSearchTypeConverter.INSTANCE.convert(typeDefine).getDataType();
    }

    @Override
    public String toConnectorType(
            String field,
            SeaTunnelDataType<?> seaTunnelDataType,
            Map<String, Object> dataTypeProperties) {
        checkNotNull(seaTunnelDataType, "seaTunnelDataType can not be null");
        Column column =
                PhysicalColumn.builder()
                        .name(field)
                        .dataType(seaTunnelDataType)
                        .nullable(true)
                        .build();
        BasicTypeDefine<EsType> typeDefine = ElasticSearchTypeConverter.INSTANCE.reconvert(column);
        return typeDefine.getColumnType();
    }

    @Override
    public String getIdentity() {
        return "Elasticsearch";
    }
}
