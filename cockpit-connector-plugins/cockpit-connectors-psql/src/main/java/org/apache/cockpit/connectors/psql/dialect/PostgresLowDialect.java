package org.apache.cockpit.connectors.psql.dialect;

import org.apache.cockpit.connectors.api.catalog.TableSchema;

import java.util.Optional;

public class PostgresLowDialect extends PostgresDialect {

    public PostgresLowDialect(String fieldIde) {
        this.fieldIde = fieldIde;
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, TableSchema tableSchema, String[] uniqueKeyFields) {
        return Optional.empty();
    }
}
