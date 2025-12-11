
package org.apache.cockpit.plugin.datasource.api.datasource;

import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.utils.JSONUtils;
import org.apache.cockpit.plugin.datasource.api.checker.ConnectivityCheckerFactory;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

@Slf4j
public abstract class AbstractDataSourceProcessor implements DataSourceProcessor {

    private static final Pattern IPV4_PATTERN = Pattern.compile("^[a-zA-Z0-9\\_\\-\\.\\,]+$");

    private static final Pattern IPV6_PATTERN = Pattern.compile("^[a-zA-Z0-9\\_\\-\\.\\:\\[\\]\\,]+$");

    private static final Pattern DATABASE_PATTER = Pattern.compile("^[a-zA-Z0-9\\_\\-\\.]+$");

    private static final Pattern PARAMS_PATTER = Pattern.compile("^[a-zA-Z0-9\\-\\_\\/\\@\\.\\:]+$");

    private static final Set<String> POSSIBLE_MALICIOUS_KEYS = Sets.newHashSet("allowLoadLocalInfile");
    private ConnectionParam connectionParam;

    @Override
    public void checkDatasourceParam(BaseDataSourceParamDTO baseDataSourceParamDTO) {
        if (!baseDataSourceParamDTO.getType().equals(DbType.REDSHIFT)) {
            // due to redshift use not regular hosts
            checkHost(baseDataSourceParamDTO.getHost());
        }
        checkDatabasePatter(baseDataSourceParamDTO.getDatabase());
        checkOther(baseDataSourceParamDTO.getOtherAsMap());
    }

    /**
     * Check the host is valid
     *
     * @param host datasource host
     */
    protected void checkHost(String host) {
        if (com.google.common.net.InetAddresses.isInetAddress(host)) {

        } else if (!IPV4_PATTERN.matcher(host).matches() || !IPV6_PATTERN.matcher(host).matches()) {
            throw new IllegalArgumentException("datasource host illegal");
        }
    }

    /**
     * check database name is valid
     *
     * @param database database name
     */
    protected void checkDatabasePatter(String database) {
        if (!DATABASE_PATTER.matcher(database).matches()) {
            throw new IllegalArgumentException("database name illegal");
        }
    }

    /**
     * check other is valid
     *
     * @param other other
     */
    protected void checkOther(Map<String, String> other) {
        if (MapUtils.isEmpty(other)) {
            return;
        }

        if (!Sets.intersection(other.keySet(), POSSIBLE_MALICIOUS_KEYS).isEmpty()) {
            throw new IllegalArgumentException("Other params include possible malicious keys.");
        }

        for (Map.Entry<String, String> entry : other.entrySet()) {
            if (!PARAMS_PATTER.matcher(entry.getKey()).matches()) {
                throw new IllegalArgumentException("datasource other params: " + entry.getKey() + " illegal");
            }
        }
    }

    protected Map<String, String> transformOtherParamToMap(String other) {
        if (StringUtils.isBlank(other)) {
            return Collections.emptyMap();
        }
        return JSONUtils.parseObject(other, new TypeReference<Map<String, String>>() {
        });
    }

    @Override
    public String getDatasourceUniqueId(ConnectionParam connectionParam, DbType dbType) {
        BaseConnectionParam baseConnectionParam = (BaseConnectionParam) connectionParam;
        return MessageFormat.format("{0}@{1}@{2}@{3}", dbType.getName(), baseConnectionParam.getUsername(),
                PasswordUtils.encodePassword(baseConnectionParam.getPassword()), baseConnectionParam.getJdbcUrl());
    }

    @Override
    public boolean checkDataSourceConnectivity(ConnectionParam connectionParam) {
        this.connectionParam = connectionParam;
        return ConnectivityCheckerFactory.getChecker(connectionParam.getDbType()).checkConnectivity(connectionParam, this);
    }

    @Override
    public List<String> splitAndRemoveComment(String sql) {
        String cleanSQL = SQLParserUtils.removeComment(sql, com.alibaba.druid.DbType.other);
        return SQLParserUtils.split(cleanSQL, com.alibaba.druid.DbType.other);
    }


    protected String getTableName(ResultSet rs) throws SQLException {
        String schemaName = rs.getString(1);
        String tableName = rs.getString(2);
        if (StringUtils.isNotBlank(schemaName)) {
            return schemaName + "." + tableName;
        }
        return null;
    }

}
