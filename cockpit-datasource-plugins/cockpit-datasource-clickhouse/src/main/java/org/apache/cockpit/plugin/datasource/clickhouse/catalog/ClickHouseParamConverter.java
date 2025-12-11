package org.apache.cockpit.plugin.datasource.clickhouse.catalog;



import org.apache.cockpit.common.constant.Constant;
import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.utils.JSONUtils;
import org.apache.cockpit.plugin.datasource.api.constants.DataSourceConstants;
import org.apache.cockpit.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.clickhouse.param.ClickHouseConnectionParam;
import org.apache.cockpit.plugin.datasource.clickhouse.param.ClickHouseDataSourceParamDTO;
import org.apache.commons.collections.MapUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ClickHouseParamConverter {

    public static ClickHouseDataSourceParamDTO createParamDTOFromConnection(String connectionJson) {
        ClickHouseConnectionParam connectionParams = JSONUtils.parseObject(connectionJson, ClickHouseConnectionParam.class);
        ClickHouseDataSourceParamDTO paramDTO = new ClickHouseDataSourceParamDTO();

        paramDTO.setUsername(connectionParams.getUsername());
        paramDTO.setDatabase(connectionParams.getDatabase());
        paramDTO.setOther(connectionParams.getOther());

        parseHostAndPort(connectionParams.getAddress(), paramDTO);

        return paramDTO;
    }

    public static BaseConnectionParam createConnectionParamFromDTO(BaseDataSourceParamDTO dataSourceParam) {
        ClickHouseDataSourceParamDTO clickHouseParam = (ClickHouseDataSourceParamDTO) dataSourceParam;

        ClickHouseConnectionParam connectionParam = new ClickHouseConnectionParam();
        connectionParam.setDatabase(clickHouseParam.getDatabase());
        connectionParam.setUsername(clickHouseParam.getUsername());
        connectionParam.setPassword(PasswordUtils.encodePassword(clickHouseParam.getPassword()));
        connectionParam.setDriverClassName(DataSourceConstants.COM_CLICKHOUSE_JDBC_DRIVER);
        connectionParam.setDriverLocation(clickHouseParam.getDriverLocation());
        connectionParam.setValidationQuery(DataSourceConstants.CLICKHOUSE_VALIDATION_QUERY);
        connectionParam.setOther(clickHouseParam.getOther());
        connectionParam.setHost(clickHouseParam.getHost());
        connectionParam.setPort(clickHouseParam.getPort());

        String jdbcUrl = buildJdbcUrl(clickHouseParam);
        connectionParam.setJdbcUrl(jdbcUrl);
        connectionParam.setAddress(buildAddress(clickHouseParam));

        connectionParam.setDbType(DbType.CLICKHOUSE);

        return connectionParam;
    }

    private static void parseHostAndPort(String address, ClickHouseDataSourceParamDTO paramDTO) {
        String[] hostSeperator = address.split(Constant.DOUBLE_SLASH);
        String[] hostPortArray = hostSeperator[hostSeperator.length - 1].split(Constant.COMMA);
        String[] hostPort = hostPortArray[0].split(Constant.COLON);

        paramDTO.setHost(hostPort[0]);
        paramDTO.setPort(Integer.parseInt(hostPort[1]));
    }

    private static String buildJdbcUrl(ClickHouseDataSourceParamDTO param) {
        String baseUrl = String.format("%s%s:%s/%s",
                DataSourceConstants.JDBC_CLICKHOUSE,
                param.getHost(),
                param.getPort(),
                param.getDatabase());

        if (MapUtils.isNotEmpty(param.getOtherAsMap())) {
            return String.format("%s?%s", baseUrl, buildQueryString(param.getOtherAsMap()));
        }
        return baseUrl;
    }

    private static String buildAddress(ClickHouseDataSourceParamDTO param) {
        return String.format("%s%s:%s",
                DataSourceConstants.JDBC_CLICKHOUSE,
                param.getHost(),
                param.getPort());
    }

    private static String buildQueryString(Map<String, String> params) {
        List<String> queryParams = new ArrayList<>();
        params.forEach((key, value) -> queryParams.add(String.format("%s=%s", key, value)));
        return String.join("&", queryParams);
    }
}