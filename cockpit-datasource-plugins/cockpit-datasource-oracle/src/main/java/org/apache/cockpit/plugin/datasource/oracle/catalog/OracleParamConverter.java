package org.apache.cockpit.plugin.datasource.oracle.catalog;


import org.apache.cockpit.common.constant.Constant;
import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.enums.DbConnectType;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.utils.JSONUtils;
import org.apache.cockpit.plugin.datasource.api.constants.DataSourceConstants;
import org.apache.cockpit.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.oracle.param.OracleConnectionParam;
import org.apache.cockpit.plugin.datasource.oracle.param.OracleDataSourceParamDTO;

public class OracleParamConverter {

    public static OracleDataSourceParamDTO createParamDTOFromConnection(String connectionJson) {
        OracleConnectionParam connectionParams = JSONUtils.parseObject(connectionJson, OracleConnectionParam.class);
        OracleDataSourceParamDTO oracleDatasourceParamDTO = new OracleDataSourceParamDTO();

        oracleDatasourceParamDTO.setDatabase(connectionParams.getDatabase());
        oracleDatasourceParamDTO.setUsername(connectionParams.getUsername());
        oracleDatasourceParamDTO.setOther(connectionParams.getOther());
        oracleDatasourceParamDTO.setConnectType(connectionParams.getConnectType());
        oracleDatasourceParamDTO.setDriverLocation(connectionParams.getDriverLocation());

        String hostSeperator = Constant.DOUBLE_SLASH;
        if (DbConnectType.ORACLE_SID.equals(connectionParams.getConnectType())) {
            hostSeperator = Constant.AT_SIGN;
        }
        String[] hostPort = connectionParams.getAddress().split(hostSeperator);
        String[] hostPortArray = hostPort[hostPort.length - 1].split(Constant.COMMA);
        oracleDatasourceParamDTO.setPort(Integer.parseInt(hostPortArray[0].split(Constant.COLON)[1]));
        oracleDatasourceParamDTO.setHost(hostPortArray[0].split(Constant.COLON)[0]);

        return oracleDatasourceParamDTO;
    }

    public static BaseConnectionParam createConnectionParamFromDTO(BaseDataSourceParamDTO dataSourceParam) {
        OracleDataSourceParamDTO oracleParam = (OracleDataSourceParamDTO) dataSourceParam;
        String address;
        String jdbcUrl;

        if (DbConnectType.ORACLE_SID.equals(oracleParam.getConnectType())) {
            address = String.format("%s%s:%s",
                    DataSourceConstants.JDBC_ORACLE_SID, oracleParam.getHost(), oracleParam.getPort());
            jdbcUrl = address + ":" + oracleParam.getDatabase();
        } else {
            address = String.format("%s%s:%s",
                    DataSourceConstants.JDBC_ORACLE_SERVICE_NAME, oracleParam.getHost(), oracleParam.getPort());
            jdbcUrl = address + "/" + oracleParam.getDatabase();
        }

        OracleConnectionParam oracleConnectionParam = new OracleConnectionParam();
        oracleConnectionParam.setUsername(oracleParam.getUsername());
        oracleConnectionParam.setPassword(PasswordUtils.encodePassword(oracleParam.getPassword()));
        oracleConnectionParam.setAddress(address);
        oracleConnectionParam.setJdbcUrl(jdbcUrl);
        oracleConnectionParam.setDatabase(oracleParam.getDatabase());
        oracleConnectionParam.setConnectType(oracleParam.getConnectType());
        oracleConnectionParam.setDriverClassName(DataSourceConstants.COM_ORACLE_JDBC_DRIVER);
        oracleConnectionParam.setValidationQuery(DataSourceConstants.ORACLE_VALIDATION_QUERY);
        oracleConnectionParam.setOther(oracleParam.getOther());
        oracleConnectionParam.setDriverLocation(oracleParam.getDriverLocation());
        oracleConnectionParam.setDbType(DbType.ORACLE);
        return oracleConnectionParam;
    }
}
