package org.apache.cockpit.connectors.hive3.dialect;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.connectors.api.catalog.TableSchema;
import org.apache.cockpit.connectors.api.jdbc.converter.BasicTypeDefine;
import org.apache.cockpit.connectors.api.jdbc.converter.JdbcRowConverter;
import org.apache.cockpit.connectors.api.jdbc.converter.TypeConverter;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialect;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialectTypeMapper;
import org.apache.cockpit.connectors.api.jdbc.dialect.dialectenum.FieldIdeEnum;
import org.apache.cockpit.connectors.api.jdbc.source.JdbcSourceTable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class Hive3Dialect implements JdbcDialect {

    public String fieldIde = FieldIdeEnum.ORIGINAL.getValue();

    public Hive3Dialect(String fieldIde) {
        this.fieldIde = fieldIde;
    }

    @Override
    public String dialectName() {
        return DbType.HIVE3.getCode();
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new HiveJdbcRowConverter();
    }

    @Override
    public TypeConverter<BasicTypeDefine> getTypeConverter() {
        return (TypeConverter) HiveTypeConverter.DEFAULT_INSTANCE;
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new HiveTypeMapper();
    }

    @Override
    public String hashModForField(String nativeType, String fieldName, int mod) {
        return null;
    }

    @Override
    public String hashModForField(String fieldName, int mod) {
        return null;
    }


    @Override
    public Optional<String> getUpsertStatement(String database, String tableName, TableSchema tableSchema, String[] uniqueKeyFields) {
        return Optional.empty();
    }

    @Override
    public String getFieldIde(String identifier, String fieldIde) {
        return fieldIde;
    }


    @Override
    public void connectionUrlParse(String url, Map<String, String> info, Map<String, String> defaultParameter) {

    }

    @Override
    public Long approximateRowCntStatement(Connection connection, JdbcSourceTable table) throws SQLException {
        return null;
    }

    @Override
    public Object[] sampleDataFromColumn(Connection connection, JdbcSourceTable table, String columnName, int samplingRate, int fetchSize) throws Exception {
        return new Object[0];
    }

    @Override
    public String sqlClauseWithDefaultValue(BasicTypeDefine columnDefine, String sourceDialectName) {
        return null;
    }

    @Override
    public boolean supportDefaultValue(BasicTypeDefine columnDefine) {
        return false;
    }

    @Override
    public boolean needsQuotesWithDefaultValue(BasicTypeDefine columnDefine) {
        return false;
    }

    @Override
    public boolean isSpecialDefaultValue(Object defaultValue, String sourceDialectName) {
        return false;
    }


    @Override
    public String getCollationSequence(Connection connection, String collate) {
        return null;
    }

    @Override
    public String getCollateSql(String collate) {
        return null;
    }

    @Override
    public String dualTable() {
        return null;
    }
}
