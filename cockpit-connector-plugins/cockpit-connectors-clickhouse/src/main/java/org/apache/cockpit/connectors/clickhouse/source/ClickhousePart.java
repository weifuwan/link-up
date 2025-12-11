package org.apache.cockpit.connectors.clickhouse.source;



import org.apache.cockpit.connectors.clickhouse.shard.Shard;

import java.io.Serializable;
import java.util.Objects;

public class ClickhousePart implements Serializable, Comparable<ClickhousePart> {

    /** SerialVersionUID */
    private static final long serialVersionUID = 2735091038047635015L;

    private final String name;
    private final String database;
    private final String table;
    private final Shard shard;
    private int offset = 0;

    /** Flag indicating whether all data from this part has been completely read. */
    private boolean isEndOfPart = false;

    public ClickhousePart(String name, String database, String table, Shard shard) {
        this.name = name;
        this.database = database;
        this.table = table;
        this.shard = shard;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public Shard getShard() {
        return shard;
    }

    public String getName() {
        return name;
    }

    public boolean isEndOfPart() {
        return isEndOfPart;
    }

    public void setEndOfPart(boolean endOfPart) {
        this.isEndOfPart = endOfPart;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getOffset() {
        return offset;
    }

    @Override
    public int compareTo(ClickhousePart o) {
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClickhousePart that = (ClickhousePart) o;
        return Objects.equals(name, that.name)
                && Objects.equals(database, that.database)
                && Objects.equals(table, that.table)
                && Objects.equals(shard, that.shard);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, database, table, shard);
    }

    @Override
    public String toString() {
        return "ClickhousePart{"
                + "name='"
                + name
                + '\''
                + ", database='"
                + database
                + '\''
                + ", table='"
                + table
                + '\''
                + ", shard="
                + shard
                + ", offset="
                + offset
                + ", isEndOfPart="
                + isEndOfPart
                + '}';
    }
}
