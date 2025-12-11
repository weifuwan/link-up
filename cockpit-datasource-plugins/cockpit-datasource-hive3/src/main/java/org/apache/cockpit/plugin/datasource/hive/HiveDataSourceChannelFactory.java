package org.apache.cockpit.plugin.datasource.hive;

import com.google.auto.service.AutoService;
import org.apache.cockpit.common.spi.datasource.DataSourceChannel;
import org.apache.cockpit.common.spi.datasource.DataSourceChannelFactory;
import org.apache.cockpit.common.spi.enums.DbType;


@AutoService(DataSourceChannelFactory.class)
public class HiveDataSourceChannelFactory implements DataSourceChannelFactory {

    @Override
    public String getName() {
        return DbType.HIVE3.getName();
    }

    @Override
    public DataSourceChannel create() {
        return new HiveDataSourceChannel();
    }
}
