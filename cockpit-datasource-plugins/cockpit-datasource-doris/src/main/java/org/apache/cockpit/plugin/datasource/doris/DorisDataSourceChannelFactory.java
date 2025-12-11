package org.apache.cockpit.plugin.datasource.doris;

import com.google.auto.service.AutoService;
import org.apache.cockpit.common.spi.datasource.DataSourceChannel;
import org.apache.cockpit.common.spi.datasource.DataSourceChannelFactory;
import org.apache.cockpit.common.spi.enums.DbType;


@AutoService(DataSourceChannelFactory.class)
public class DorisDataSourceChannelFactory implements DataSourceChannelFactory {

    @Override
    public DataSourceChannel create() {
        return new DorisDataSourceChannel();
    }

    @Override
    public String getName() {
        return DbType.DORIS.getName();
    }
}
