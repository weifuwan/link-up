package org.apache.cockpit.plugin.datasource.elasticsearch.param;

import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.plugin.datasource.api.datasource.BaseDataSourceParamDTO;

public class ElasticSearchDataSourceParamDTO extends BaseDataSourceParamDTO {

    private String scheme;
    private String pathPrefix;

    public String getScheme() {
        return scheme;
    }

    public void setScheme(String scheme) {
        this.scheme = scheme;
    }

    public String getPathPrefix() {
        return pathPrefix;
    }

    public void setPathPrefix(String pathPrefix) {
        this.pathPrefix = pathPrefix;
    }

    @Override
    public String toString() {
        return "ElasticSearchDataSourceParamDTO{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", scheme='" + scheme + '\'' +
                ", pathPrefix='" + pathPrefix + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", other='" + other + '\'' +
                '}';
    }

    @Override
    public DbType getType() {
        return DbType.ELASTICSEARCH;
    }
}
