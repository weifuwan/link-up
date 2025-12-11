package org.apache.cockpit.connectors.api.jdbc.config;

import lombok.Builder;
import lombok.Data;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.jdbc.catalog.JdbcCatalogOptions;

import java.io.Serializable;
import java.util.List;

import static org.apache.cockpit.connectors.api.jdbc.config.JdbcOptions.*;


@Data
@Builder
public class JdbcSinkConfig implements Serializable {
    private static final long serialVersionUID = 2L;

    private JdbcConnectionConfig jdbcConnectionConfig;
    private boolean isExactlyOnce;
    private String simpleSql;
    private String database;
    private String table;
    private List<String> primaryKeys;
    private boolean enableUpsert;
    @Builder.Default private boolean isPrimaryKeyUpdated = true;
    private boolean supportUpsertByInsertOnly;
    private boolean useCopyStatement;
    @Builder.Default private boolean createIndex = true;

    public static JdbcSinkConfig of(ReadonlyConfig config) {
        JdbcSinkConfigBuilder builder = JdbcSinkConfig.builder();
        builder.jdbcConnectionConfig(JdbcConnectionConfig.of(config));
        builder.isExactlyOnce(config.get(JdbcOptions.IS_EXACTLY_ONCE));
        config.getOptional(JdbcOptions.PRIMARY_KEYS).ifPresent(builder::primaryKeys);
        config.getOptional(JdbcOptions.DATABASE).ifPresent(builder::database);
        config.getOptional(JdbcOptions.TABLE).ifPresent(builder::table);
        builder.enableUpsert(config.get(ENABLE_UPSERT));
        builder.isPrimaryKeyUpdated(config.get(IS_PRIMARY_KEY_UPDATED));
        builder.supportUpsertByInsertOnly(config.get(SUPPORT_UPSERT_BY_INSERT_ONLY));
        builder.simpleSql(config.get(JdbcOptions.QUERY));
        builder.useCopyStatement(config.get(JdbcOptions.USE_COPY_STATEMENT));
        builder.createIndex(config.get(JdbcCatalogOptions.CREATE_INDEX));
        return builder.build();
    }
}
