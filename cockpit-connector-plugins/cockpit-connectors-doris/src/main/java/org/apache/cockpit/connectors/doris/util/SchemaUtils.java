package org.apache.cockpit.connectors.doris.util;

import org.apache.cockpit.connectors.doris.rest.models.Field;
import org.apache.cockpit.connectors.doris.rest.models.Schema;
import org.apache.doris.sdk.thrift.TScanColumnDesc;

import java.util.List;

public class SchemaUtils {

    /**
     * convert Doris return schema to inner schema struct.
     *
     * @param tscanColumnDescs Doris BE return schema
     * @return inner schema struct
     */
    public static Schema convertToSchema(List<TScanColumnDesc> tscanColumnDescs) {
        Schema schema = new Schema(tscanColumnDescs.size());
        tscanColumnDescs.stream()
                .forEach(
                        desc ->
                                schema.put(
                                        new Field(
                                                desc.getName(),
                                                desc.getType().name(),
                                                "",
                                                0,
                                                0,
                                                "")));
        return schema;
    }
}
