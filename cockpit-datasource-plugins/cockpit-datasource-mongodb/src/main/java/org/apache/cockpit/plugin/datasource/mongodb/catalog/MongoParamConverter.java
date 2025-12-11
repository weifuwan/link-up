package org.apache.cockpit.plugin.datasource.mongodb.catalog;


import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.utils.JSONUtils;
import org.apache.cockpit.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.mongodb.param.MongoConnectionParam;
import org.apache.cockpit.plugin.datasource.mongodb.param.MongoDataSourceParamDTO;

public class MongoParamConverter {

    public static MongoDataSourceParamDTO createParamDTOFromConnection(String connectionJson) {
        MongoConnectionParam connectionParams = JSONUtils.parseObject(connectionJson, MongoConnectionParam.class);
        MongoDataSourceParamDTO paramDTO = new MongoDataSourceParamDTO();

        paramDTO.setUsername(connectionParams.getUsername());
        paramDTO.setDatabase(connectionParams.getDatabase());
        paramDTO.setOther(connectionParams.getOther());

        parseHostAndPort(connectionParams.getAddress(), paramDTO);

        return paramDTO;
    }

    public static BaseConnectionParam createConnectionParamFromDTO(BaseDataSourceParamDTO dataSourceParam) {
        MongoDataSourceParamDTO mongoParam = (MongoDataSourceParamDTO) dataSourceParam;

        MongoConnectionParam connectionParam = new MongoConnectionParam();
        connectionParam.setDatabase(mongoParam.getDatabase());
        connectionParam.setUsername(mongoParam.getUsername());
        connectionParam.setPassword(PasswordUtils.encodePassword(mongoParam.getPassword()));
        connectionParam.setOther(mongoParam.getOther());
        connectionParam.setJdbcUrl(buildConnectionString(mongoParam));

        String address = buildAddress(mongoParam);
        connectionParam.setAddress(address);
        connectionParam.setConnectionString(buildConnectionString(mongoParam));
        connectionParam.setHost(mongoParam.getHost());
        connectionParam.setPort(mongoParam.getPort());

        return connectionParam;
    }

    private static void parseHostAndPort(String address, MongoDataSourceParamDTO paramDTO) {
        if (address != null && !address.isEmpty()) {
            String[] hostPort = address.split(":");
            if (hostPort.length >= 2) {
                paramDTO.setHost(hostPort[0]);
                paramDTO.setPort(Integer.parseInt(hostPort[1]));
            }
        }
    }

    private static String buildConnectionString(MongoDataSourceParamDTO param) {
        StringBuilder connectionString = new StringBuilder("mongodb://");

        if (param.getUsername() != null && !param.getUsername().isEmpty()) {
            connectionString.append(param.getUsername());
            if (param.getPassword() != null && !param.getPassword().isEmpty()) {
                connectionString.append(":").append(param.getPassword());
            }
            connectionString.append("@");
        }

        connectionString.append(param.getHost()).append(":").append(param.getPort());

        if (param.getDatabase() != null && !param.getDatabase().isEmpty()) {
            connectionString.append("/").append(param.getDatabase());
        }

        return connectionString.toString();
    }

    private static String buildAddress(MongoDataSourceParamDTO param) {
        return param.getHost() + ":" + param.getPort();
    }
}
