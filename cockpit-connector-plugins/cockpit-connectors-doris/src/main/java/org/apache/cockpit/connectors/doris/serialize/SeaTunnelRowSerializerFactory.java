package org.apache.cockpit.connectors.doris.serialize;


import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.doris.config.DorisSinkConfig;
import org.apache.cockpit.connectors.doris.sink.writer.LoadConstants;

public class SeaTunnelRowSerializerFactory {

    /**
     * Create a DorisSerializer instance
     *
     * @param dorisSinkConfig dorisSinkConfig
     * @param seaTunnelRowType seaTunnelRowType
     * @return DorisSerializer
     */
    public static DorisSerializer createSerializer(
            DorisSinkConfig dorisSinkConfig, SeaTunnelRowType seaTunnelRowType) {
        return new SeaTunnelRowSerializer(
                dorisSinkConfig
                        .getStreamLoadProps()
                        .getProperty(LoadConstants.FORMAT_KEY)
                        .toLowerCase(),
                seaTunnelRowType,
                dorisSinkConfig.getStreamLoadProps().getProperty(LoadConstants.FIELD_DELIMITER_KEY),
                dorisSinkConfig.getEnableDelete(),
                dorisSinkConfig.isCaseSensitive());
    }
}
