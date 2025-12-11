package org.apache.cockpit.plugin.datasource.db2.catalog;

import org.apache.cockpit.common.constant.Constant;
import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.utils.JSONUtils;
import org.apache.cockpit.plugin.datasource.api.constants.DataSourceConstants;
import org.apache.cockpit.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.db2.param.DB2ConnectionParam;
import org.apache.cockpit.plugin.datasource.db2.param.DB2DataSourceParamDTO;
import org.apache.commons.collections.MapUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DB2ParamConverter {

    public static DB2DataSourceParamDTO createParamDTOFromConnection(String connectionJson) {
        DB2ConnectionParam connectionParams = JSONUtils.parseObject(connectionJson, DB2ConnectionParam.class);
        DB2DataSourceParamDTO paramDTO = new DB2DataSourceParamDTO();

        paramDTO.setUsername(connectionParams.getUsername());
        paramDTO.setDatabase(connectionParams.getDatabase());
        paramDTO.setOther(connectionParams.getOther());

        parseHostAndPort(connectionParams.getAddress(), paramDTO);

        return paramDTO;
    }

    public static BaseConnectionParam createConnectionParamFromDTO(BaseDataSourceParamDTO dataSourceParam) {
        DB2DataSourceParamDTO db2Param = (DB2DataSourceParamDTO) dataSourceParam;

        DB2ConnectionParam connectionParam = new DB2ConnectionParam();
        connectionParam.setDatabase(db2Param.getDatabase());
        connectionParam.setUsername(db2Param.getUsername());
        connectionParam.setPassword(PasswordUtils.encodePassword(db2Param.getPassword()));
        connectionParam.setDriverClassName(DataSourceConstants.COM_DB2_JDBC_DRIVER);
        connectionParam.setDriverLocation(db2Param.getDriverLocation());
        connectionParam.setValidationQuery(DataSourceConstants.DB2_VALIDATION_QUERY);
        connectionParam.setDbType(DbType.DB2);
        connectionParam.setOther(db2Param.getOther());

        String jdbcUrl = buildJdbcUrl(db2Param);
        connectionParam.setJdbcUrl(jdbcUrl);
        connectionParam.setAddress(buildAddress(db2Param));

        return connectionParam;
    }

    private static void parseHostAndPort(String address, DB2DataSourceParamDTO paramDTO) {
        String[] hostSeperator = address.split(Constant.DOUBLE_SLASH);
        String[] hostPortArray = hostSeperator[hostSeperator.length - 1].split(Constant.COMMA);
        String[] hostPort = hostPortArray[0].split(Constant.COLON);

        paramDTO.setHost(hostPort[0]);
        paramDTO.setPort(Integer.parseInt(hostPort[1]));
    }

    private static String buildJdbcUrl(DB2DataSourceParamDTO param) {
        String baseUrl = String.format("%s%s:%s/%s",
                DataSourceConstants.JDBC_DB2,
                param.getHost(),
                param.getPort(),
                param.getDatabase());

        if (MapUtils.isNotEmpty(param.getOtherAsMap())) {
            return String.format("%s:%s", baseUrl, buildQueryString(param.getOtherAsMap()));
        }
        return baseUrl;
    }

    private static String buildAddress(DB2DataSourceParamDTO param) {
        return String.format("%s%s:%s",
                DataSourceConstants.JDBC_DB2,
                param.getHost(),
                param.getPort());
    }

    private static String buildQueryString(Map<String, String> params) {
        List<String> queryParams = new ArrayList<>();
        params.forEach((key, value) -> queryParams.add(String.format("%s=%s", key, value)));
        return String.join("&", queryParams);
    }
}
