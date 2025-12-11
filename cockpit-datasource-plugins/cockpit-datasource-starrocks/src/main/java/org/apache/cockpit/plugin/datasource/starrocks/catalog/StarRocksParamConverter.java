package org.apache.cockpit.plugin.datasource.starrocks.catalog;


import org.apache.cockpit.common.constant.Constant;
import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.utils.JSONUtils;
import org.apache.cockpit.plugin.datasource.api.constants.DataSourceConstants;
import org.apache.cockpit.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.starrocks.param.StarRocksConnectionParam;
import org.apache.cockpit.plugin.datasource.starrocks.param.StarRocksDataSourceParamDTO;
import org.apache.commons.collections.MapUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StarRocksParamConverter {

    public static StarRocksDataSourceParamDTO createParamDTOFromConnection(String connectionJson) {
        StarRocksConnectionParam connectionParams = JSONUtils.parseObject(connectionJson, StarRocksConnectionParam.class);
        StarRocksDataSourceParamDTO paramDTO = new StarRocksDataSourceParamDTO();

        paramDTO.setUsername(connectionParams.getUsername());
        paramDTO.setDatabase(connectionParams.getDatabase());
        paramDTO.setOther(connectionParams.getOther());

        parseHostAndPort(connectionParams.getAddress(), paramDTO);

        return paramDTO;
    }

    public static BaseConnectionParam createConnectionParamFromDTO(BaseDataSourceParamDTO dataSourceParam) {
        StarRocksDataSourceParamDTO starRocksParam = (StarRocksDataSourceParamDTO) dataSourceParam;

        StarRocksConnectionParam connectionParam = new StarRocksConnectionParam();
        connectionParam.setDatabase(starRocksParam.getDatabase());
        connectionParam.setUsername(starRocksParam.getUsername());
        connectionParam.setPassword(PasswordUtils.encodePassword(starRocksParam.getPassword()));
        connectionParam.setDriverClassName(DataSourceConstants.COM_MYSQL_CJ_JDBC_DRIVER); // StarRocks使用MySQL协议
        connectionParam.setDriverLocation(starRocksParam.getDriverLocation());
        connectionParam.setValidationQuery(DataSourceConstants.MYSQL_VALIDATION_QUERY);
        connectionParam.setOther(starRocksParam.getOther());

        String jdbcUrl = buildJdbcUrl(starRocksParam);
        connectionParam.setJdbcUrl(jdbcUrl);
        connectionParam.setAddress(buildAddress(starRocksParam));
        connectionParam.setHost(starRocksParam.getHost());
        connectionParam.setPort(starRocksParam.getPort());

        connectionParam.setDbType(DbType.STARROCKS);

        return connectionParam;
    }

    private static void parseHostAndPort(String address, StarRocksDataSourceParamDTO paramDTO) {
        String[] hostSeperator = address.split(Constant.DOUBLE_SLASH);
        String[] hostPortArray = hostSeperator[hostSeperator.length - 1].split(Constant.COMMA);
        String[] hostPort = hostPortArray[0].split(Constant.COLON);

        paramDTO.setHost(hostPort[0]);
        paramDTO.setPort(Integer.parseInt(hostPort[1]));
    }

    private static String buildJdbcUrl(StarRocksDataSourceParamDTO param) {
        String baseUrl = String.format("%s%s:%s/%s",
                DataSourceConstants.JDBC_MYSQL, // StarRocks使用MySQL协议
                param.getHost(),
                param.getPort(),
                param.getDatabase());

        if (MapUtils.isNotEmpty(param.getOtherAsMap())) {
            return String.format("%s?%s", baseUrl, buildQueryString(param.getOtherAsMap()));
        }
        return baseUrl;
    }

    private static String buildAddress(StarRocksDataSourceParamDTO param) {
        return String.format("%s%s:%s",
                DataSourceConstants.JDBC_MYSQL,
                param.getHost(),
                param.getPort());
    }

    private static String buildQueryString(Map<String, String> params) {
        List<String> queryParams = new ArrayList<>();
        params.forEach((key, value) -> queryParams.add(String.format("%s=%s", key, value)));
        return String.join("&", queryParams);
    }
}
