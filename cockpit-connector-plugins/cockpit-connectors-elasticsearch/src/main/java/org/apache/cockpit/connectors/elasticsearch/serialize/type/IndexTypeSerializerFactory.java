package org.apache.cockpit.connectors.elasticsearch.serialize.type;


import org.apache.cockpit.connectors.elasticsearch.constant.ElasticsearchVersion;
import org.apache.cockpit.connectors.elasticsearch.dto.ElasticsearchClusterInfo;
import org.apache.cockpit.connectors.elasticsearch.serialize.type.impl.NotIndexTypeSerializer;
import org.apache.cockpit.connectors.elasticsearch.serialize.type.impl.RequiredIndexTypeSerializer;

import static org.apache.cockpit.connectors.elasticsearch.constant.ElasticsearchVersion.*;

public class IndexTypeSerializerFactory {

    private static final String DEFAULT_TYPE = "st";

    private IndexTypeSerializerFactory() {
    }

    public static IndexTypeSerializer getIndexTypeSerializer(
            ElasticsearchClusterInfo elasticsearchClusterInfo, String type) {
        if (elasticsearchClusterInfo.isOpensearch()) {
            return new NotIndexTypeSerializer();
        }
        ElasticsearchVersion elasticsearchVersion =
                elasticsearchClusterInfo.getElasticsearchVersion();
        if (elasticsearchVersion == ES2 || elasticsearchVersion == ES5) {
            if (type == null || "".equals(type)) {
                type = DEFAULT_TYPE;
            }
            return new RequiredIndexTypeSerializer(type);
        }
        if (elasticsearchVersion == ES6) {
            if (type != null && !"".equals(type)) {
                return new RequiredIndexTypeSerializer(type);
            }
        }
        return new NotIndexTypeSerializer();
    }
}
