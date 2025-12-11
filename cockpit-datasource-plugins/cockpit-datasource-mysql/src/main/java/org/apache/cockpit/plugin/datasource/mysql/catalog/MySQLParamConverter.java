package org.apache.cockpit.plugin.datasource.mysql.catalog;

import org.apache.cockpit.common.constant.Constant;
import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.utils.JSONUtils;
import org.apache.cockpit.plugin.datasource.api.constants.DataSourceConstants;
import org.apache.cockpit.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.mysql.param.MySQLConnectionParam;
import org.apache.cockpit.plugin.datasource.mysql.param.MySQLDataSourceParamDTO;
import org.apache.commons.collections.MapUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MySQLParamConverter {

    public static MySQLDataSourceParamDTO createParamDTOFromConnection(String connectionJson) {
        MySQLConnectionParam connectionParams = JSONUtils.parseObject(connectionJson, MySQLConnectionParam.class);
        MySQLDataSourceParamDTO paramDTO = new MySQLDataSourceParamDTO();

        paramDTO.setUsername(connectionParams.getUsername());
        paramDTO.setDatabase(connectionParams.getDatabase());
        paramDTO.setOther(connectionParams.getOther());

        parseHostAndPort(connectionParams.getAddress(), paramDTO);


        return paramDTO;
    }

    public static BaseConnectionParam createConnectionParamFromDTO(BaseDataSourceParamDTO dataSourceParam) {
        MySQLDataSourceParamDTO mysqlParam = (MySQLDataSourceParamDTO) dataSourceParam;

        MySQLConnectionParam connectionParam = new MySQLConnectionParam();
        connectionParam.setDatabase(mysqlParam.getDatabase());
        connectionParam.setUsername(mysqlParam.getUsername());
        connectionParam.setPassword(PasswordUtils.encodePassword(mysqlParam.getPassword()));
        connectionParam.setDriverClassName(DataSourceConstants.COM_MYSQL_CJ_JDBC_DRIVER);
        connectionParam.setDriverLocation(mysqlParam.getDriverLocation());
        connectionParam.setValidationQuery(DataSourceConstants.MYSQL_VALIDATION_QUERY);
        connectionParam.setOther(mysqlParam.getOther());

        String jdbcUrl = buildJdbcUrl(mysqlParam);
        connectionParam.setJdbcUrl(jdbcUrl);
        connectionParam.setAddress(buildAddress(mysqlParam));
        connectionParam.setDbType(DbType.MYSQL);
        return connectionParam;
    }

    private static void parseHostAndPort(String address, MySQLDataSourceParamDTO paramDTO) {
        String[] hostSeperator = address.split(Constant.DOUBLE_SLASH);
        String[] hostPortArray = hostSeperator[hostSeperator.length - 1].split(Constant.COMMA);
        String[] hostPort = hostPortArray[0].split(Constant.COLON);

        paramDTO.setHost(hostPort[0]);
        paramDTO.setPort(Integer.parseInt(hostPort[1]));
    }

    private static String buildJdbcUrl(MySQLDataSourceParamDTO param) {
        String baseUrl = String.format("%s%s:%s/%s",
                DataSourceConstants.JDBC_MYSQL,
                param.getHost(),
                param.getPort(),
                param.getDatabase());

        if (MapUtils.isNotEmpty(param.getOtherAsMap())) {
            return String.format("%s?%s", baseUrl, buildQueryString(param.getOtherAsMap()));
        }
        return baseUrl;
    }

    private static String buildAddress(MySQLDataSourceParamDTO param) {
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
