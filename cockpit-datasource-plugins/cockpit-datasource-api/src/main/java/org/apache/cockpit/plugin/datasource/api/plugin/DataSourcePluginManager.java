
package org.apache.cockpit.plugin.datasource.api.plugin;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.datasource.DataSourceChannel;
import org.apache.cockpit.common.spi.datasource.DataSourceChannelFactory;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.spi.plugin.PrioritySPIFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;

@Slf4j
public class DataSourcePluginManager {

    private final Map<String, DataSourceChannel> datasourceChannelMap = new ConcurrentHashMap<>();

    public DataSourceChannel getDataSourceChannel(final DbType dbType) {
        return datasourceChannelMap.get(dbType.getName());
    }

    public void installPlugin() {

        PrioritySPIFactory<DataSourceChannelFactory> prioritySPIFactory =
                new PrioritySPIFactory<>(DataSourceChannelFactory.class);
        for (Map.Entry<String, DataSourceChannelFactory> entry : prioritySPIFactory.getSPIMap().entrySet()) {
            final DataSourceChannelFactory factory = entry.getValue();
            final String name = entry.getKey();

            log.info("Registering datasource plugin: {}", name);

            if (datasourceChannelMap.containsKey(name)) {
                throw new IllegalStateException(format("Duplicate datasource plugins named '%s'", name));
            }

            loadDatasourceClient(factory);

            log.info("Registered datasource plugin: {}", name);
        }
    }

    private void loadDatasourceClient(DataSourceChannelFactory datasourceChannelFactory) {
        DataSourceChannel datasourceChannel = datasourceChannelFactory.create();
        datasourceChannelMap.put(datasourceChannelFactory.getName(), datasourceChannel);
    }
}
