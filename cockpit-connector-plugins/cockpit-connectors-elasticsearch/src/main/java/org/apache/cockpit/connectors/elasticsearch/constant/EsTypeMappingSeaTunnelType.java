package org.apache.cockpit.connectors.elasticsearch.constant;


import org.apache.cockpit.connectors.api.type.BasicType;
import org.apache.cockpit.connectors.api.type.LocalTimeType;
import org.apache.cockpit.connectors.api.type.SeaTunnelDataType;
import org.apache.cockpit.connectors.elasticsearch.exception.ElasticsearchConnectorErrorCode;
import org.apache.cockpit.connectors.elasticsearch.exception.ElasticsearchConnectorException;

import java.util.HashMap;
import java.util.Map;

public class EsTypeMappingSeaTunnelType {

    private static final Map<String, SeaTunnelDataType> MAPPING =
            new HashMap() {
                {
                    put("string", BasicType.STRING_TYPE);
                    put("keyword", BasicType.STRING_TYPE);
                    put("text", BasicType.STRING_TYPE);
                    put("binary", BasicType.STRING_TYPE);
                    put("boolean", BasicType.BOOLEAN_TYPE);
                    put("byte", BasicType.BYTE_TYPE);
                    put("short", BasicType.SHORT_TYPE);
                    put("integer", BasicType.INT_TYPE);
                    put("long", BasicType.LONG_TYPE);
                    put("float", BasicType.FLOAT_TYPE);
                    put("half_float", BasicType.FLOAT_TYPE);
                    put("double", BasicType.DOUBLE_TYPE);
                    put("date", LocalTimeType.LOCAL_DATE_TIME_TYPE);
                }
            };

    /**
     * if not find the mapping SeaTunnelDataType will throw runtime exception
     *
     */
    public static SeaTunnelDataType getSeaTunnelDataType(String esType) {
        SeaTunnelDataType seaTunnelDataType = MAPPING.get(esType);
        if (seaTunnelDataType == null) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.ES_FIELD_TYPE_NOT_SUPPORT,
                    String.format("elasticsearch type is %s", esType));
        }
        return seaTunnelDataType;
    }
}
