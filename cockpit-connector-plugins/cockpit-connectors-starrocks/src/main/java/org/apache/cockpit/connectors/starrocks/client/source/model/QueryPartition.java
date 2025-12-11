package org.apache.cockpit.connectors.starrocks.client.source.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

@Setter
@Getter
@AllArgsConstructor
public class QueryPartition implements Serializable, Comparable<QueryPartition> {
    private final String database;
    private final String table;

    private final String beAddress;
    private final Set<Long> tabletIds;
    private final String queryPlan;

    @Override
    public int compareTo(QueryPartition o) {
        int cmp = database.compareTo(o.database);
        if (cmp != 0) {
            return cmp;
        }
        cmp = table.compareTo(o.table);
        if (cmp != 0) {
            return cmp;
        }
        cmp = beAddress.compareTo(o.beAddress);
        if (cmp != 0) {
            return cmp;
        }
        cmp = queryPlan.compareTo(o.queryPlan);
        if (cmp != 0) {
            return cmp;
        }

        cmp = tabletIds.size() - o.tabletIds.size();
        if (cmp != 0) {
            return cmp;
        }

        Set<Long> similar = new HashSet<>(tabletIds);
        Set<Long> diffSelf = new HashSet<>(tabletIds);
        Set<Long> diffOther = new HashSet<>(o.tabletIds);
        similar.retainAll(o.tabletIds);
        diffSelf.removeAll(similar);
        diffOther.removeAll(similar);
        if (diffSelf.size() == 0) {
            return 0;
        }
        long diff = Collections.min(diffSelf) - Collections.min(diffOther);
        return diff < 0 ? -1 : 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryPartition that = (QueryPartition) o;
        return Objects.equals(database, that.database)
                && Objects.equals(table, that.table)
                && Objects.equals(beAddress, that.beAddress)
                && Objects.equals(tabletIds, that.tabletIds)
                && Objects.equals(queryPlan, that.queryPlan);
    }

    @Override
    public int hashCode() {
        int result = database.hashCode();
        result = 31 * result + table.hashCode();
        result = 31 * result + beAddress.hashCode();
        result = 31 * result + tabletIds.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "QueryPartition{"
                + "database='"
                + database
                + '\''
                + ", table='"
                + table
                + '\''
                + ", beAddress='"
                + beAddress
                + '\''
                + ", tabletIds="
                + tabletIds
                + ", queryPlan='"
                + queryPlan
                + '\''
                + '}';
    }
}
