package org.apache.cockpit.connectors.api.jdbc.dialect;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.catalog.TableSchema;
import org.apache.cockpit.connectors.api.jdbc.converter.AbstractJdbcRowConverter;
import org.apache.cockpit.connectors.api.jdbc.converter.JdbcRowConverter;
import org.apache.cockpit.connectors.api.jdbc.dialect.dialectenum.FieldIdeEnum;

import java.util.Optional;

@Slf4j
public class GenericDialect implements JdbcDialect {

    public String fieldIde = FieldIdeEnum.ORIGINAL.getValue();

    public GenericDialect() {}

    public GenericDialect(String fieldIde) {
        this.fieldIde = fieldIde;
    }

    @Override
    public String dialectName() {
        return DatabaseIdentifier.GENERIC;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new AbstractJdbcRowConverter() {
            @Override
            public String converterName() {
                return DatabaseIdentifier.GENERIC;
            }
        };
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new GenericTypeMapper();
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return getFieldIde(identifier, fieldIde);
    }

    @Override
    public String quoteDatabaseIdentifier(String identifier) {
        return identifier;
    }

    @Override
    public String tableIdentifier(TablePath tablePath) {
        return tableIdentifier(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, TableSchema tableSchema, String[] uniqueKeyFields) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TablePath parse(String tablePath) {
        return TablePath.of(tablePath, false);
    }
}
