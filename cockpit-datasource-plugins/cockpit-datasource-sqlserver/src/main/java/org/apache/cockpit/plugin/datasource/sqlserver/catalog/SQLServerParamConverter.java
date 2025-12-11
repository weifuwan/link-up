package org.apache.cockpit.plugin.datasource.sqlserver.catalog;


import org.apache.cockpit.common.constant.Constant;
import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.utils.JSONUtils;
import org.apache.cockpit.plugin.datasource.api.constants.DataSourceConstants;
import org.apache.cockpit.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.sqlserver.param.SQLServerConnectionParam;
import org.apache.cockpit.plugin.datasource.sqlserver.param.SQLServerDataSourceParamDTO;

public class SQLServerParamConverter {

    public static SQLServerDataSourceParamDTO createParamDTOFromConnection(String connectionJson) {
        SQLServerConnectionParam connectionParams = (SQLServerConnectionParam) JSONUtils.parseObject(connectionJson, SQLServerConnectionParam.class);
        String[] hostSeperator = connectionParams.getAddress().split(Constant.DOUBLE_SLASH);
        String[] hostPortArray = hostSeperator[hostSeperator.length - 1].split(Constant.COMMA);

        SQLServerDataSourceParamDTO sqlServerDatasourceParamDTO = new SQLServerDataSourceParamDTO();
        sqlServerDatasourceParamDTO.setDatabase(connectionParams.getDatabase());
        sqlServerDatasourceParamDTO.setUsername(connectionParams.getUsername());
        sqlServerDatasourceParamDTO.setOther(connectionParams.getOther());
        sqlServerDatasourceParamDTO.setPort(Integer.parseInt(hostPortArray[0].split(Constant.COLON)[1]));
        sqlServerDatasourceParamDTO.setHost(hostPortArray[0].split(Constant.COLON)[0]);
        sqlServerDatasourceParamDTO.setDriverLocation(connectionParams.getDriverLocation());
        sqlServerDatasourceParamDTO.setSchema(connectionParams.getSchema());
        return sqlServerDatasourceParamDTO;
    }

    public static BaseConnectionParam createConnectionParamFromDTO(BaseDataSourceParamDTO dataSourceParam) {
        SQLServerDataSourceParamDTO sqlServerParam = (SQLServerDataSourceParamDTO) dataSourceParam;
        String address = String.format("%s%s:%s", DataSourceConstants.JDBC_SQLSERVER, sqlServerParam.getHost(),
                sqlServerParam.getPort());
        String jdbcUrl = address + ";databaseName=" + sqlServerParam.getDatabase();

        SQLServerConnectionParam sqlServerConnectionParam = new SQLServerConnectionParam();
        sqlServerConnectionParam.setAddress(address);
        sqlServerConnectionParam.setDatabase(sqlServerParam.getDatabase());
        sqlServerConnectionParam.setJdbcUrl(jdbcUrl);
        sqlServerConnectionParam.setOther(sqlServerParam.getOther());
        sqlServerConnectionParam.setUsername(sqlServerParam.getUsername());
        sqlServerConnectionParam.setPassword(PasswordUtils.encodePassword(sqlServerParam.getPassword()));
        sqlServerConnectionParam.setDriverClassName(DataSourceConstants.COM_SQLSERVER_JDBC_DRIVER);
        sqlServerConnectionParam.setValidationQuery(DataSourceConstants.SQLSERVER_VALIDATION_QUERY);
        sqlServerConnectionParam.setDriverLocation(sqlServerParam.getDriverLocation());
        sqlServerConnectionParam.setSchema(sqlServerParam.getSchema());
        return sqlServerConnectionParam;
    }
}