package org.apache.cockpit.connectors.elasticsearch.serialize.source;


import org.apache.cockpit.connectors.api.type.SeaTunnelRow;

public interface SeaTunnelRowDeserializer {

    SeaTunnelRow deserialize(ElasticsearchRecord rowRecord);
}
