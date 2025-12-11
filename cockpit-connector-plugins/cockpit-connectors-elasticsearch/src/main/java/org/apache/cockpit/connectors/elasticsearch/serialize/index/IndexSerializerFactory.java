package org.apache.cockpit.connectors.elasticsearch.serialize.index;



import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.elasticsearch.serialize.index.impl.FixedValueIndexSerializer;
import org.apache.cockpit.connectors.elasticsearch.serialize.index.impl.VariableIndexSerializer;
import org.apache.cockpit.connectors.elasticsearch.util.RegexUtils;

import java.util.List;

public class IndexSerializerFactory {

    public static IndexSerializer getIndexSerializer(
            String index, SeaTunnelRowType seaTunnelRowType) {
        List<String> fieldNames = RegexUtils.extractDatas(index, "\\$\\{(.*?)\\}");
        if (fieldNames != null && fieldNames.size() > 0) {
            return new VariableIndexSerializer(seaTunnelRowType, index, fieldNames);
        } else {
            return new FixedValueIndexSerializer(index);
        }
    }
}
