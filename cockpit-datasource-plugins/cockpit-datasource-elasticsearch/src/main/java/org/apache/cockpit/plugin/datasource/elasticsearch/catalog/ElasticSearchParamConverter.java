package org.apache.cockpit.plugin.datasource.elasticsearch.catalog;

import org.apache.cockpit.common.constant.Constant;
import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.utils.JSONUtils;
import org.apache.cockpit.plugin.datasource.api.constants.DataSourceConstants;
import org.apache.cockpit.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.elasticsearch.param.ElasticSearchConnectionParam;
import org.apache.cockpit.plugin.datasource.elasticsearch.param.ElasticSearchDataSourceParamDTO;
import org.apache.commons.collections.MapUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ElasticSearchParamConverter {

    public static ElasticSearchDataSourceParamDTO createParamDTOFromConnection(String connectionJson) {
        ElasticSearchConnectionParam connectionParams = JSONUtils.parseObject(connectionJson, ElasticSearchConnectionParam.class);
        ElasticSearchDataSourceParamDTO paramDTO = new ElasticSearchDataSourceParamDTO();

        paramDTO.setUsername(connectionParams.getUsername());
        paramDTO.setDatabase(connectionParams.getDatabase());
        paramDTO.setOther(connectionParams.getOther());
        paramDTO.setScheme(connectionParams.getScheme());
        paramDTO.setPathPrefix(connectionParams.getPathPrefix());
        paramDTO.setHost(connectionParams.getHost());
        paramDTO.setPort(connectionParams.getPort());

        parseHostAndPort(connectionParams.getAddress(), paramDTO);

        return paramDTO;
    }

    public static BaseConnectionParam createConnectionParamFromDTO(BaseDataSourceParamDTO dataSourceParam) {
        ElasticSearchDataSourceParamDTO esParam = (ElasticSearchDataSourceParamDTO) dataSourceParam;

        ElasticSearchConnectionParam connectionParam = new ElasticSearchConnectionParam();
        connectionParam.setDatabase(esParam.getDatabase());
        connectionParam.setUsername(esParam.getUsername());
        connectionParam.setPassword(PasswordUtils.encodePassword(esParam.getPassword()));
        connectionParam.setDriverClassName(DataSourceConstants.ELASTICSEARCH_DRIVER);
        connectionParam.setDriverLocation(esParam.getDriverLocation());
        connectionParam.setValidationQuery(DataSourceConstants.ELASTICSEARCH_VALIDATION_QUERY);
        connectionParam.setOther(esParam.getOther());
        connectionParam.setScheme(esParam.getScheme());
        connectionParam.setPathPrefix(esParam.getPathPrefix());

        connectionParam.setHost(esParam.getHost());
        connectionParam.setPort(esParam.getPort());

        String jdbcUrl = buildJdbcUrl(esParam);
        connectionParam.setJdbcUrl(jdbcUrl);
        connectionParam.setAddress(buildAddress(esParam));

        return connectionParam;
    }

    private static void parseHostAndPort(String address, ElasticSearchDataSourceParamDTO paramDTO) {
        String[] hostSeperator = address.split(Constant.DOUBLE_SLASH);
        String[] hostPortArray = hostSeperator[hostSeperator.length - 1].split(Constant.COMMA);
        String[] hostPort = hostPortArray[0].split(Constant.COLON);

        paramDTO.setHost(hostPort[0]);
        paramDTO.setPort(Integer.parseInt(hostPort[1]));
    }

    private static String buildJdbcUrl(ElasticSearchDataSourceParamDTO param) {
        StringBuilder urlBuilder = new StringBuilder();
        urlBuilder.append(DataSourceConstants.JDBC_ELASTICSEARCH_PREFIX);

        if (param.getScheme() != null) {
            urlBuilder.append(param.getScheme()).append("://");
        } else {
            urlBuilder.append("http://");
        }

        urlBuilder.append(param.getHost()).append(":").append(param.getPort());

        if (param.getPathPrefix() != null) {
            urlBuilder.append("/").append(param.getPathPrefix());
        }

        if (MapUtils.isNotEmpty(param.getOtherAsMap())) {
            urlBuilder.append("?").append(buildQueryString(param.getOtherAsMap()));
        }

        return urlBuilder.toString();
    }

    private static String buildAddress(ElasticSearchDataSourceParamDTO param) {
        StringBuilder addressBuilder = new StringBuilder();
        addressBuilder.append(DataSourceConstants.JDBC_ELASTICSEARCH_PREFIX);

        if (param.getScheme() != null) {
            addressBuilder.append(param.getScheme()).append("://");
        } else {
            addressBuilder.append("http://");
        }

        addressBuilder.append(param.getHost()).append(":").append(param.getPort());

        return addressBuilder.toString();
    }

    private static String buildQueryString(Map<String, String> params) {
        List<String> queryParams = new ArrayList<>();
        params.forEach((key, value) -> queryParams.add(String.format("%s=%s", key, value)));
        return String.join("&", queryParams);
    }
}
