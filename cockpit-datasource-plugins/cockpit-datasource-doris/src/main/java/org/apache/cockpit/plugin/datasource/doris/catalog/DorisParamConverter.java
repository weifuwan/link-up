package org.apache.cockpit.plugin.datasource.doris.catalog;


import org.apache.cockpit.common.constant.Constant;
import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.utils.JSONUtils;
import org.apache.cockpit.plugin.datasource.api.constants.DataSourceConstants;
import org.apache.cockpit.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.doris.param.DorisConnectionParam;
import org.apache.cockpit.plugin.datasource.doris.param.DorisDataSourceParamDTO;

public class DorisParamConverter {

    public static DorisDataSourceParamDTO createParamDTOFromConnection(String connectionJson) throws NumberFormatException {
        DorisConnectionParam connectionParams = (DorisConnectionParam) JSONUtils.parseObject(connectionJson, DorisConnectionParam.class);
        DorisDataSourceParamDTO dorisDataSourceParamDTO = new DorisDataSourceParamDTO();

        dorisDataSourceParamDTO.setUsername(connectionParams.getUsername());
        dorisDataSourceParamDTO.setDatabase(connectionParams.getDatabase());
        dorisDataSourceParamDTO.setOther(connectionParams.getOther());
        dorisDataSourceParamDTO.setDriverLocation(connectionParams.getDriverLocation());

        String address = connectionParams.getAddress();
        String[] hostSeperator = address.split(Constant.DOUBLE_SLASH);
        String[] hostPortArrays = hostSeperator[hostSeperator.length - 1].split(Constant.COMMA);

        dorisDataSourceParamDTO.setPort(Integer.parseInt(hostPortArrays[0].split(Constant.COLON)[1]));

        for (int i = 0; i < hostPortArrays.length; i++) {
            hostPortArrays[i] = hostPortArrays[i].split(Constant.COLON)[0];
        }
        dorisDataSourceParamDTO.setHost(String.join(",", hostPortArrays));

        return dorisDataSourceParamDTO;
    }

    public static BaseConnectionParam createConnectionParamFromDTO(BaseDataSourceParamDTO dataSourceParam) {
        DorisDataSourceParamDTO dorisDataSourceParamDTO = (DorisDataSourceParamDTO) dataSourceParam;
        String[] hosts = dataSourceParam.getHost().split(Constant.COMMA);

        for (int i = 0; i < hosts.length; i++) {
            hosts[i] = String.format(Constant.FORMAT_S_S_COLON, hosts[i], dorisDataSourceParamDTO.getPort());
        }

        String address = String.format("%s%s", DataSourceConstants.JDBC_MYSQL_LOADBALANCE, String.join(",", hosts));
        String jdbcUrl = String.format(Constant.FORMAT_S_S, address, dorisDataSourceParamDTO.getDatabase());

        DorisConnectionParam dorisConnectionParam = new DorisConnectionParam();
        dorisConnectionParam.setJdbcUrl(jdbcUrl);
        dorisConnectionParam.setDatabase(dorisDataSourceParamDTO.getDatabase());
        dorisConnectionParam.setAddress(address);
        dorisConnectionParam.setUsername(dorisDataSourceParamDTO.getUsername());
        dorisConnectionParam.setPassword(PasswordUtils.encodePassword(dorisDataSourceParamDTO.getPassword()));
        dorisConnectionParam.setDriverClassName(DataSourceConstants.COM_MYSQL_CJ_JDBC_DRIVER);
        dorisConnectionParam.setValidationQuery(DataSourceConstants.MYSQL_VALIDATION_QUERY);
        dorisConnectionParam.setOther(dorisDataSourceParamDTO.getOther());
        dorisConnectionParam.setDriverLocation(dorisDataSourceParamDTO.getDriverLocation());
        dorisConnectionParam.setHost(dorisDataSourceParamDTO.getHost());
        dorisConnectionParam.setDbType(DbType.DORIS);
        return dorisConnectionParam;
    }
}
