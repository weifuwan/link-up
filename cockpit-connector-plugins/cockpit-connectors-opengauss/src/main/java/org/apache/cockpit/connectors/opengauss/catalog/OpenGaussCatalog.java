package org.apache.cockpit.connectors.opengauss.catalog;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.util.JdbcUrlUtil;
import org.apache.cockpit.connectors.psql.catalog.PostgresCatalog;

import java.sql.Connection;

@Slf4j
public class OpenGaussCatalog extends PostgresCatalog {

    public OpenGaussCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema,
            String driverClass,
            String driverLocation) {
        super(catalogName, username, pwd, urlInfo, defaultSchema, driverClass, driverLocation);
    }

    @VisibleForTesting
    public void setConnection(String url, Connection connection) {
        this.connectionMap.put(url, connection);
    }
}
