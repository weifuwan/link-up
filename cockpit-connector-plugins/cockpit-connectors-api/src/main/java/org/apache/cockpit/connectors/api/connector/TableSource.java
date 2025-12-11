package org.apache.cockpit.connectors.api.connector;


import org.apache.cockpit.connectors.api.source.SeaTunnelSource;
import org.apache.cockpit.connectors.api.source.SourceSplit;


public interface TableSource<T, SplitT extends SourceSplit> {

    SeaTunnelSource<T, SplitT> createSource();
}
