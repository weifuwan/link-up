
package org.apache.cockpit.plugin.datasource.api.plugin;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.plugin.datasource.api.datasource.DataSourceProcessor;

import java.util.Map;

@Slf4j
public class DataSourceProcessorProvider {

    private static final DataSourceProcessorManager dataSourcePluginManager = new DataSourceProcessorManager();

    static {
        dataSourcePluginManager.installProcessor();
    }

    private DataSourceProcessorProvider() {
    }

    public static void initialize() {
        log.info("Initialize DataSourceProcessorProvider");
    }

    public static DataSourceProcessor getDataSourceProcessor(@NonNull DbType dbType) {
        return dataSourcePluginManager.getDataSourceProcessorMap().get(dbType.name());
    }

    public static Map<String, DataSourceProcessor> getDataSourceProcessorMap() {
        return dataSourcePluginManager.getDataSourceProcessorMap();
    }

}
