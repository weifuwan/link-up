package org.apache.cockpit.plugin.datasource.elasticsearch.param;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;

@Data
@EqualsAndHashCode(callSuper = true)
public class ElasticSearchConnectionParam extends BaseConnectionParam {

    private String scheme;
    private String pathPrefix;
    private String host;
    private Integer port;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

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
        return "ElasticSearchConnectionParam{" +
                "username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", address='" + address + '\'' +
                ", database='" + database + '\'' +
                ", jdbcUrl='" + jdbcUrl + '\'' +
                ", scheme='" + scheme + '\'' +
                ", pathPrefix='" + pathPrefix + '\'' +
                ", driverLocation='" + driverLocation + '\'' +
                ", driverClassName='" + driverClassName + '\'' +
                ", validationQuery='" + validationQuery + '\'' +
                ", other='" + other + '\'' +
                '}';
    }
}
