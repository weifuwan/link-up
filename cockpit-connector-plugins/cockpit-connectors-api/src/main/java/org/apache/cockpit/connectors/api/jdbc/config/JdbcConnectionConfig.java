package org.apache.cockpit.connectors.api.jdbc.config;

import lombok.Getter;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Getter
public class JdbcConnectionConfig implements Serializable {
    private static final long serialVersionUID = 2L;

    private String jdbcUrl;
    private String driverName;
    private String compatibleMode;
    private int connectionCheckTimeoutSeconds =
            JdbcOptions.CONNECTION_CHECK_TIMEOUT_SEC.defaultValue();
    private int maxRetries = JdbcOptions.MAX_RETRIES.defaultValue();
    private String username;
    private String password;
    private String query;

    // 新增driver location
    private String driverLocation;

    private boolean autoCommit = JdbcOptions.AUTO_COMMIT.defaultValue();

    private int batchSize = JdbcOptions.BATCH_SIZE.defaultValue();

    private String xaDataSourceClassName;

    private boolean decimalTypeNarrowing = JdbcOptions.DECIMAL_TYPE_NARROWING.defaultValue();

    private int maxCommitAttempts = JdbcOptions.MAX_COMMIT_ATTEMPTS.defaultValue();

    private int transactionTimeoutSec = JdbcOptions.TRANSACTION_TIMEOUT_SEC.defaultValue();

    private boolean useKerberos = JdbcOptions.USE_KERBEROS.defaultValue();

    private String kerberosPrincipal;

    private String kerberosKeytabPath;

    private String krb5Path = JdbcOptions.KRB5_PATH.defaultValue();

    private String dialect = JdbcOptions.DIALECT.defaultValue();

    private Map<String, String> properties;

    private boolean handleBlobAsString = JdbcOptions.HANDLE_BLOB_AS_STRING.defaultValue();

    public static JdbcConnectionConfig of(ReadonlyConfig config) {
        Builder builder = JdbcConnectionConfig.builder();
        builder.jdbcUrl(config.get(JdbcOptions.JDBC_URL));
        builder.compatibleMode(config.get(JdbcOptions.COMPATIBLE_MODE));
        builder.driverName(config.get(JdbcOptions.DRIVER));
        builder.autoCommit(config.get(JdbcOptions.AUTO_COMMIT));
        builder.maxRetries(config.get(JdbcOptions.MAX_RETRIES));
        builder.connectionCheckTimeoutSeconds(config.get(JdbcOptions.CONNECTION_CHECK_TIMEOUT_SEC));
        builder.batchSize(config.get(JdbcOptions.BATCH_SIZE));
        builder.handleBlobAsString(config.get(JdbcOptions.HANDLE_BLOB_AS_STRING));
        builder.driverLocation(config.get(JdbcOptions.DRIVER_LOCATION));
        if (config.get(JdbcOptions.IS_EXACTLY_ONCE)) {
            builder.xaDataSourceClassName(config.get(JdbcOptions.XA_DATA_SOURCE_CLASS_NAME));
            builder.maxCommitAttempts(config.get(JdbcOptions.MAX_COMMIT_ATTEMPTS));
            builder.transactionTimeoutSec(config.get(JdbcOptions.TRANSACTION_TIMEOUT_SEC));
            builder.maxRetries(0);
        }
        if (config.get(JdbcOptions.USE_KERBEROS)) {
            builder.useKerberos(config.get(JdbcOptions.USE_KERBEROS));
            builder.kerberosPrincipal(config.get(JdbcOptions.KERBEROS_PRINCIPAL));
            builder.kerberosKeytabPath(config.get(JdbcOptions.KERBEROS_KEYTAB_PATH));
            builder.krb5Path(config.get(JdbcOptions.KRB5_PATH));
        }
        config.getOptional(JdbcOptions.USERNAME).ifPresent(builder::username);
        config.getOptional(JdbcOptions.PASSWORD).ifPresent(builder::password);
        config.getOptional(JdbcOptions.PROPERTIES).ifPresent(builder::properties);
        config.getOptional(JdbcOptions.DECIMAL_TYPE_NARROWING)
                .ifPresent(builder::decimalTypeNarrowing);
        config.getOptional(JdbcOptions.DIALECT).ifPresent(builder::dialect);
        return builder.build();
    }

    public Optional<String> getUsername() {
        return Optional.ofNullable(username);
    }

    public Optional<String> getPassword() {
        return Optional.ofNullable(password);
    }

    public Optional<Integer> getTransactionTimeoutSec() {
        return transactionTimeoutSec < 0 ? Optional.empty() : Optional.of(transactionTimeoutSec);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String jdbcUrl;
        private String driverName;
        private String compatibleMode;
        private int connectionCheckTimeoutSeconds =
                JdbcOptions.CONNECTION_CHECK_TIMEOUT_SEC.defaultValue();
        private int maxRetries = JdbcOptions.MAX_RETRIES.defaultValue();
        private String username;
        private String password;
        private String query;
        private boolean autoCommit = JdbcOptions.AUTO_COMMIT.defaultValue();
        private int batchSize = JdbcOptions.BATCH_SIZE.defaultValue();
        private String xaDataSourceClassName;
        private boolean decimalTypeNarrowing = JdbcOptions.DECIMAL_TYPE_NARROWING.defaultValue();
        private boolean handleBlobAsString = JdbcOptions.HANDLE_BLOB_AS_STRING.defaultValue();
        private int maxCommitAttempts = JdbcOptions.MAX_COMMIT_ATTEMPTS.defaultValue();
        private int transactionTimeoutSec = JdbcOptions.TRANSACTION_TIMEOUT_SEC.defaultValue();
        private Map<String, String> properties;
        public boolean useKerberos = JdbcOptions.USE_KERBEROS.defaultValue();
        public String kerberosPrincipal;
        public String kerberosKeytabPath;
        public String krb5Path = JdbcOptions.KRB5_PATH.defaultValue();
        public String dialect = JdbcOptions.DIALECT.defaultValue();

        private String driverLocation;


        private Builder() {
        }

        public Builder jdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        public Builder driverLocation(String driverLocation) {
            this.driverLocation = driverLocation;
            return this;
        }


        public Builder driverName(String driverName) {
            this.driverName = driverName;
            return this;
        }

        public Builder compatibleMode(String compatibleMode) {
            this.compatibleMode = compatibleMode;
            return this;
        }

        public Builder connectionCheckTimeoutSeconds(int connectionCheckTimeoutSeconds) {
            this.connectionCheckTimeoutSeconds = connectionCheckTimeoutSeconds;
            return this;
        }

        public Builder decimalTypeNarrowing(boolean decimalTypeNarrowing) {
            this.decimalTypeNarrowing = decimalTypeNarrowing;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder query(String query) {
            this.query = query;
            return this;
        }

        public Builder autoCommit(boolean autoCommit) {
            this.autoCommit = autoCommit;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder xaDataSourceClassName(String xaDataSourceClassName) {
            this.xaDataSourceClassName = xaDataSourceClassName;
            return this;
        }

        public Builder maxCommitAttempts(int maxCommitAttempts) {
            this.maxCommitAttempts = maxCommitAttempts;
            return this;
        }

        public Builder transactionTimeoutSec(int transactionTimeoutSec) {
            this.transactionTimeoutSec = transactionTimeoutSec;
            return this;
        }

        public Builder useKerberos(boolean useKerberos) {
            this.useKerberos = useKerberos;
            return this;
        }

        public Builder kerberosPrincipal(String kerberosPrincipal) {
            this.kerberosPrincipal = kerberosPrincipal;
            return this;
        }

        public Builder kerberosKeytabPath(String kerberosKeytabPath) {
            this.kerberosKeytabPath = kerberosKeytabPath;
            return this;
        }

        public Builder krb5Path(String krb5Path) {
            this.krb5Path = krb5Path;
            return this;
        }

        public Builder dialect(String dialect) {
            this.dialect = dialect;
            return this;
        }

        public Builder properties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public Builder handleBlobAsString(boolean handleBlobAsString) {
            this.handleBlobAsString = handleBlobAsString;
            return this;
        }

        public JdbcConnectionConfig build() {
            JdbcConnectionConfig jdbcConnectionConfig = new JdbcConnectionConfig();
            jdbcConnectionConfig.batchSize = this.batchSize;
            jdbcConnectionConfig.driverName = this.driverName;
            jdbcConnectionConfig.compatibleMode = this.compatibleMode;
            jdbcConnectionConfig.maxRetries = this.maxRetries;
            jdbcConnectionConfig.password = this.password;
            jdbcConnectionConfig.connectionCheckTimeoutSeconds = this.connectionCheckTimeoutSeconds;
            jdbcConnectionConfig.jdbcUrl = this.jdbcUrl;
            jdbcConnectionConfig.autoCommit = this.autoCommit;
            jdbcConnectionConfig.username = this.username;
            jdbcConnectionConfig.transactionTimeoutSec = this.transactionTimeoutSec;
            jdbcConnectionConfig.maxCommitAttempts = this.maxCommitAttempts;
            jdbcConnectionConfig.xaDataSourceClassName = this.xaDataSourceClassName;
            jdbcConnectionConfig.decimalTypeNarrowing = this.decimalTypeNarrowing;
            jdbcConnectionConfig.handleBlobAsString = this.handleBlobAsString;
            jdbcConnectionConfig.useKerberos = this.useKerberos;
            jdbcConnectionConfig.kerberosPrincipal = this.kerberosPrincipal;
            jdbcConnectionConfig.kerberosKeytabPath = this.kerberosKeytabPath;
            jdbcConnectionConfig.krb5Path = this.krb5Path;
            jdbcConnectionConfig.dialect = this.dialect;
            jdbcConnectionConfig.properties =
                    this.properties == null ? new HashMap<>() : this.properties;
            jdbcConnectionConfig.driverLocation = this.driverLocation;
            return jdbcConnectionConfig;
        }
    }

    public boolean isHandleBlobAsString() {
        return handleBlobAsString;
    }

    public void setHandleBlobAsString(boolean handleBlobAsString) {
        this.handleBlobAsString = handleBlobAsString;
    }
}
