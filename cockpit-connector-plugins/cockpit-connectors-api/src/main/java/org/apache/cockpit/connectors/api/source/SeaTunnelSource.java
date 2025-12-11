package org.apache.cockpit.connectors.api.source;

import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.common.PluginIdentifierInterface;
import org.apache.cockpit.connectors.api.type.SeaTunnelDataType;

import java.io.Serializable;
import java.util.List;


public interface SeaTunnelSource<T, SplitT extends SourceSplit>
        extends Serializable,
        PluginIdentifierInterface {

    /**
     * Get the boundedness of this source.
     *
     * @return the boundedness of this source.
     */
    Boundedness getBoundedness();

    /**
     * Get the data type of the records produced by this source.
     *
     * @return SeaTunnel data type.
     * @deprecated Please use {@link #getProducedCatalogTables}
     */
    @Deprecated
    default SeaTunnelDataType<T> getProducedType() {
        return (SeaTunnelDataType) getProducedCatalogTables().get(0).getSeaTunnelRowType();
    }

    /**
     * Get the catalog tables output by this source, It is recommended that all connectors implement
     * this method instead of {@link #getProducedType}. CatalogTable contains more information to
     * help downstream support more accurate and complete synchronization capabilities.
     */
    default List<CatalogTable> getProducedCatalogTables() {
        throw new UnsupportedOperationException(
                "getProducedCatalogTables method has not been implemented.");
    }

    /**
     * Create source reader, used to produce data.
     *
     * @param readerContext reader context.
     * @return source reader.
     * @throws Exception when create reader failed.
     */
    SourceReader<T, SplitT> createReader(SourceReader.Context readerContext) throws Exception;


}
