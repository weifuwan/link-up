package org.apache.cockpit.connectors.api.catalog;


import org.apache.cockpit.connectors.api.type.SeaTunnelDataType;

import java.util.Map;

/**
 * @param <T>
 */
@Deprecated
public interface DataTypeConvertor<T> {

    /**
     * Transfer the data type from connector to SeaTunnel.
     *
     * @param field The field name of the column
     * @param connectorDataType e.g. "int", "varchar(255)"
     * @return the data type of SeaTunnel
     */
    SeaTunnelDataType<?> toSeaTunnelType(String field, String connectorDataType);

    /**
     * Transfer the data type from connector to SeaTunnel.
     *
     * @param field The field name of the column
     * @param connectorDataType origin data type
     * @param dataTypeProperties origin data type properties, e.g. precision, scale, length
     * @return SeaTunnel data type
     */
    // todo: If the origin data type contains the properties, we can remove the dataTypeProperties.
    SeaTunnelDataType<?> toSeaTunnelType(
            String field, T connectorDataType, Map<String, Object> dataTypeProperties);

    /**
     * Transfer the data type from SeaTunnel to connector.
     *
     * @param field The field name of the column
     * @param seaTunnelDataType seaTunnel data type
     * @param dataTypeProperties seaTunnel data type properties, e.g. precision, scale, length
     * @return origin data type
     */
    // todo: If the SeaTunnel data type contains the properties, we can remove the
    // dataTypeProperties.
    T toConnectorType(
            String field,
            SeaTunnelDataType<?> seaTunnelDataType,
            Map<String, Object> dataTypeProperties);

    String getIdentity();
}
