package org.apache.cockpit.plugin.datasource.postgresql.catalog;

import org.apache.cockpit.common.constant.Constant;
import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.utils.JSONUtils;
import org.apache.cockpit.plugin.datasource.api.constants.DataSourceConstants;
import org.apache.cockpit.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.postgresql.param.PostgreSQLConnectionParam;
import org.apache.cockpit.plugin.datasource.postgresql.param.PostgreSQLDataSourceParamDTO;

public class PostgreSQLParamConverter {

    public static PostgreSQLDataSourceParamDTO createParamDTOFromConnection(String connectionJson) {
        PostgreSQLConnectionParam connectionParams = (PostgreSQLConnectionParam) JSONUtils.parseObject(connectionJson, PostgreSQLConnectionParam.class);
        PostgreSQLDataSourceParamDTO postgreSqlDatasourceParamDTO = new PostgreSQLDataSourceParamDTO();

        postgreSqlDatasourceParamDTO.setDatabase(connectionParams.getDatabase());
        postgreSqlDatasourceParamDTO.setUsername(connectionParams.getUsername());
        postgreSqlDatasourceParamDTO.setOther(connectionParams.getOther());

        String address = connectionParams.getAddress();
        String[] hostSeperator = address.split(Constant.DOUBLE_SLASH);
        String[] hostPortArray = hostSeperator[hostSeperator.length - 1].split(Constant.COMMA);
        postgreSqlDatasourceParamDTO.setHost(hostPortArray[0].split(Constant.COLON)[0]);
        postgreSqlDatasourceParamDTO.setPort(Integer.parseInt(hostPortArray[0].split(Constant.COLON)[1]));

        return postgreSqlDatasourceParamDTO;
    }

    public static BaseConnectionParam createConnectionParamFromDTO(BaseDataSourceParamDTO dataSourceParam) {
        PostgreSQLDataSourceParamDTO postgreSqlParam = (PostgreSQLDataSourceParamDTO) dataSourceParam;
        String address = String.format("%s%s:%s", DataSourceConstants.JDBC_POSTGRESQL, postgreSqlParam.getHost(),
                postgreSqlParam.getPort());
        String jdbcUrl = String.format("%s/%s", address, postgreSqlParam.getDatabase());

        PostgreSQLConnectionParam postgreSqlConnectionParam = new PostgreSQLConnectionParam();
        postgreSqlConnectionParam.setJdbcUrl(jdbcUrl);
        postgreSqlConnectionParam.setAddress(address);
        postgreSqlConnectionParam.setDatabase(postgreSqlParam.getDatabase());
        postgreSqlConnectionParam.setUsername(postgreSqlParam.getUsername());
        postgreSqlConnectionParam.setPassword(PasswordUtils.encodePassword(postgreSqlParam.getPassword()));
        postgreSqlConnectionParam.setDriverClassName(DataSourceConstants.ORG_POSTGRESQL_DRIVER);
        postgreSqlConnectionParam.setDriverLocation(postgreSqlParam.getDriverLocation());
        postgreSqlConnectionParam.setValidationQuery(DataSourceConstants.POSTGRESQL_VALIDATION_QUERY);
        postgreSqlConnectionParam.setOther(postgreSqlParam.getOther());
        postgreSqlConnectionParam.setSchema(postgreSqlParam.getSchema());
        postgreSqlConnectionParam.setDbType(DbType.POSTGRESQL);

        return postgreSqlConnectionParam;
    }
}