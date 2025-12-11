package org.apache.cockpit.plugin.datasource.opengauss.catalog;

import org.apache.cockpit.common.constant.Constant;
import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.spi.enums.DriverType;
import org.apache.cockpit.common.utils.JSONUtils;
import org.apache.cockpit.plugin.datasource.api.constants.DataSourceConstants;
import org.apache.cockpit.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.opengauss.param.OpenGaussConnectionParam;
import org.apache.cockpit.plugin.datasource.opengauss.param.OpenGaussDataSourceParamDTO;

public class OpenGaussParamConverter {

    public static OpenGaussDataSourceParamDTO createParamDTOFromConnection(String connectionJson) {
        OpenGaussConnectionParam connectionParams = JSONUtils.parseObject(connectionJson, OpenGaussConnectionParam.class);
        OpenGaussDataSourceParamDTO openGaussDatasourceParamDTO = new OpenGaussDataSourceParamDTO();

        openGaussDatasourceParamDTO.setDatabase(connectionParams.getDatabase());
        openGaussDatasourceParamDTO.setUsername(connectionParams.getUsername());
        openGaussDatasourceParamDTO.setOther(connectionParams.getOther());
        openGaussDatasourceParamDTO.setDriverType(connectionParams.getDriverType());
        openGaussDatasourceParamDTO.setDriverLocation(connectionParams.getDriverLocation());

        String address = connectionParams.getAddress();
        String[] hostSeperator = address.split(Constant.DOUBLE_SLASH);
        String[] hostPortArray = hostSeperator[hostSeperator.length - 1].split(Constant.COMMA);
        openGaussDatasourceParamDTO.setHost(hostPortArray[0].split(Constant.COLON)[0]);
        openGaussDatasourceParamDTO.setPort(Integer.parseInt(hostPortArray[0].split(Constant.COLON)[1]));
        openGaussDatasourceParamDTO.setSchema(connectionParams.getSchema());

        return openGaussDatasourceParamDTO;
    }

    public static BaseConnectionParam createConnectionParamFromDTO(BaseDataSourceParamDTO dataSourceParam) {
        OpenGaussDataSourceParamDTO openGaussParam = (OpenGaussDataSourceParamDTO) dataSourceParam;

        String jdbcUrlPrefix;
        if (openGaussParam.getDriverType() == DriverType.OPEN_GAUSS) {
            jdbcUrlPrefix = DataSourceConstants.JDBC_OPEN_GAUSS;
        } else {
            jdbcUrlPrefix = DataSourceConstants.JDBC_POSTGRESQL;
        }

        String address = String.format("%s%s:%s", jdbcUrlPrefix, openGaussParam.getHost(),
                openGaussParam.getPort());
        String jdbcUrl = String.format("%s/%s", address, openGaussParam.getDatabase());

        OpenGaussConnectionParam openGaussConnectionParam = new OpenGaussConnectionParam();
        openGaussConnectionParam.setJdbcUrl(jdbcUrl);
        openGaussConnectionParam.setAddress(address);
        openGaussConnectionParam.setDatabase(openGaussParam.getDatabase());
        openGaussConnectionParam.setUsername(openGaussParam.getUsername());
        openGaussConnectionParam.setPassword(PasswordUtils.encodePassword(openGaussParam.getPassword()));
        openGaussConnectionParam.setDriverClassName(DataSourceConstants.ORG_POSTGRESQL_DRIVER);
        openGaussConnectionParam.setValidationQuery(DataSourceConstants.OPENGAUSS_VALIDATION_QUERY);
        openGaussConnectionParam.setOther(openGaussParam.getOther());
        openGaussConnectionParam.setDriverType(openGaussParam.getDriverType());
        openGaussConnectionParam.setDriverLocation(openGaussParam.getDriverLocation());
        openGaussConnectionParam.setSchema(openGaussParam.getSchema());
        openGaussConnectionParam.setDbType(DbType.OPENGAUSS);
        return openGaussConnectionParam;
    }
}