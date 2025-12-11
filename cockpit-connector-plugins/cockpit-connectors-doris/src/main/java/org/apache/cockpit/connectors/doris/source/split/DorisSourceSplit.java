package org.apache.cockpit.connectors.doris.source.split;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.cockpit.connectors.api.source.SourceSplit;
import org.apache.cockpit.connectors.doris.rest.PartitionDefinition;

import java.util.Objects;

@AllArgsConstructor
@Getter
@Setter
public class DorisSourceSplit implements SourceSplit {

    private final PartitionDefinition partitionDefinition;

    private final String splitId;

    @Override
    public String splitId() {
        return splitId;
    }

    public PartitionDefinition getPartitionDefinition() {
        return partitionDefinition;
    }

    @Override
    public String toString() {
        return String.format(
                "DorisSourceSplit: %s.%s,be=%s,tablets=%s",
                partitionDefinition.getDatabase(),
                partitionDefinition.getTable(),
                partitionDefinition.getBeAddress(),
                partitionDefinition.getTabletIds());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DorisSourceSplit that = (DorisSourceSplit) o;

        return Objects.equals(partitionDefinition, that.partitionDefinition);
    }
}
