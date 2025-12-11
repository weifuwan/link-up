package org.apache.cockpit.plugin.datasource.cache.catalog;


import org.apache.cockpit.common.constant.Constant;
import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.utils.JSONUtils;
import org.apache.cockpit.plugin.datasource.api.constants.DataSourceConstants;
import org.apache.cockpit.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.cache.param.CacheConnectionParam;
import org.apache.cockpit.plugin.datasource.cache.param.CacheDataSourceParamDTO;

public class CacheParamConverter {

    public static CacheDataSourceParamDTO createParamDTOFromConnection(String connectionJson) {
        CacheConnectionParam connectionParams = (CacheConnectionParam) JSONUtils.parseObject(connectionJson, CacheConnectionParam.class);
        CacheDataSourceParamDTO cacheDatasourceParamDTO = new CacheDataSourceParamDTO();

        cacheDatasourceParamDTO.setUsername(connectionParams.getUsername());
        cacheDatasourceParamDTO.setDatabase(connectionParams.getDatabase());
        cacheDatasourceParamDTO.setOther(connectionParams.getOther());

        String address = connectionParams.getAddress();
        String[] hostSeperator = address.split(Constant.DOUBLE_SLASH);
        String[] hostPortArray = hostSeperator[hostSeperator.length - 1].split(Constant.COMMA);
        cacheDatasourceParamDTO.setPort(Integer.parseInt(hostPortArray[0].split(Constant.COLON)[1]));
        cacheDatasourceParamDTO.setHost(hostPortArray[0].split(Constant.COLON)[0]);

        return cacheDatasourceParamDTO;
    }

    public static BaseConnectionParam createConnectionParamFromDTO(BaseDataSourceParamDTO dataSourceParam) {
        CacheDataSourceParamDTO cacheDatasourceParam = (CacheDataSourceParamDTO) dataSourceParam;
        String address = String.format("%s%s:%s", DataSourceConstants.JDBC_CACHE, cacheDatasourceParam.getHost(),
                cacheDatasourceParam.getPort());
        String jdbcUrl = String.format("%s/%s", address, cacheDatasourceParam.getDatabase());

        CacheConnectionParam cacheConnectionParam = new CacheConnectionParam();
        cacheConnectionParam.setJdbcUrl(jdbcUrl);
        cacheConnectionParam.setDatabase(cacheDatasourceParam.getDatabase());
        cacheConnectionParam.setAddress(address);
        cacheConnectionParam.setUsername(cacheDatasourceParam.getUsername());
        cacheConnectionParam.setPassword(PasswordUtils.encodePassword(cacheDatasourceParam.getPassword()));
        cacheConnectionParam.setDriverClassName(DataSourceConstants.COM_CACHE_JDBC_DRIVER);
        cacheConnectionParam.setDriverLocation(dataSourceParam.getDriverLocation());
        cacheConnectionParam.setValidationQuery(DataSourceConstants.CACHE_VALIDATION_QUERY);
        cacheConnectionParam.setOther(cacheDatasourceParam.getOther());

        // 设置realJdbcUrl
        String jdbcUrlWithOther = getJdbcUrlWithOther(cacheConnectionParam);
        cacheConnectionParam.setJdbcUrl(jdbcUrlWithOther);

        cacheConnectionParam.setDbType(DbType.CACHE);

        return cacheConnectionParam;
    }

    private static String getJdbcUrlWithOther(CacheConnectionParam cacheConnectionParam) {
        // 这里需要调用CacheConnectionManager的方法，但由于是静态方法，我们复制逻辑
        if (org.apache.commons.collections.MapUtils.isNotEmpty(cacheConnectionParam.getOtherAsMap())) {
            java.util.Map<String, String> otherMap = cacheConnectionParam.getOtherAsMap();
            java.util.List<String> list = new java.util.ArrayList<>(otherMap.size());
            otherMap.forEach((key, value) -> list.add(String.format("%s=%s", key, value)));
            String queryString = String.join("&", list);
            return String.format("%s?%s", cacheConnectionParam.getJdbcUrl(), queryString);
        }
        return cacheConnectionParam.getJdbcUrl();
    }
}
