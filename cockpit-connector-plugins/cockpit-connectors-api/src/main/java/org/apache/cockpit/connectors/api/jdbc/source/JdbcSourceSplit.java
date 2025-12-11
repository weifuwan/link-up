package org.apache.cockpit.connectors.api.jdbc.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.source.SourceSplit;
import org.apache.cockpit.connectors.api.type.SeaTunnelDataType;

@Data
@ToString
@AllArgsConstructor
public class JdbcSourceSplit implements SourceSplit {
    private static final long serialVersionUID = -815542654355310611L;
    private final TablePath tablePath;
    private final String splitId;
    private final String splitQuery;
    private final String splitKeyName;
    private final SeaTunnelDataType splitKeyType;
    private final Object splitStart;
    private final Object splitEnd;

    @Override
    public String splitId() {
        return splitId;
    }
}
