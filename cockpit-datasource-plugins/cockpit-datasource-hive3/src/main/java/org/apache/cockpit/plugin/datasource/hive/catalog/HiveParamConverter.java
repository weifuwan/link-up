package org.apache.cockpit.plugin.datasource.hive.catalog;

import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.utils.JSONUtils;
import org.apache.cockpit.plugin.datasource.api.constants.DataSourceConstants;
import org.apache.cockpit.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.hive.param.HiveConnectionParam;
import org.apache.cockpit.plugin.datasource.hive.param.HiveDataSourceParamDTO;

public class HiveParamConverter {

    public static HiveDataSourceParamDTO createParamDTOFromConnection(String connectionJson) {
        HiveConnectionParam connectionParams = JSONUtils.parseObject(connectionJson, HiveConnectionParam.class);
        HiveDataSourceParamDTO paramDTO = new HiveDataSourceParamDTO();

        paramDTO.setUsername(connectionParams.getUsername());
        paramDTO.setDatabase(connectionParams.getDatabase());
        paramDTO.setOther(connectionParams.getOther());
        paramDTO.setDriverLocation(connectionParams.getDriverLocation());
        paramDTO.setHost(connectionParams.getAddress());
        return paramDTO;
    }

    public static BaseConnectionParam createConnectionParamFromDTO(BaseDataSourceParamDTO dataSourceParam) {
        HiveDataSourceParamDTO hiveParam = (HiveDataSourceParamDTO) dataSourceParam;

        HiveConnectionParam connectionParam = new HiveConnectionParam();
        connectionParam.setDatabase(hiveParam.getDatabase());
        connectionParam.setUsername(hiveParam.getUsername());
        connectionParam.setPassword(PasswordUtils.encodePassword(hiveParam.getPassword()));
        connectionParam.setDriverClassName(DataSourceConstants.ORG_APACHE_HIVE_JDBC_HIVE_DRIVER);
        connectionParam.setValidationQuery(DataSourceConstants.HIVE_VALIDATION_QUERY);
        connectionParam.setOther(hiveParam.getOther());
        connectionParam.setLoginUserKeytabUsername(hiveParam.getLoginUserKeytabUsername());
        connectionParam.setLoginUserKeytabPath(hiveParam.getLoginUserKeytabPath());
        connectionParam.setJavaSecurityKrb5Conf(hiveParam.getJavaSecurityKrb5Conf());
        connectionParam.setDriverLocation(hiveParam.getDriverLocation());

        String jdbcUrl = buildJdbcUrl(hiveParam);
        connectionParam.setJdbcUrl(jdbcUrl);
        connectionParam.setAddress(hiveParam.getHost());
        connectionParam.setHost(hiveParam.getHost());
        connectionParam.setDbType(DbType.HIVE3);
        return connectionParam;
    }

    private static String buildJdbcUrl(HiveDataSourceParamDTO param) {
        return String.format("jdbc:hive2://%s:%s/%s", param.getHost(), param.getPort(), param.getDatabase());
    }

}