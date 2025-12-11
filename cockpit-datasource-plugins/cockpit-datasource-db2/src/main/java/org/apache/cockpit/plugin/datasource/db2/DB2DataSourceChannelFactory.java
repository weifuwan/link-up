package org.apache.cockpit.plugin.datasource.db2;


import com.google.auto.service.AutoService;
import org.apache.cockpit.common.spi.datasource.DataSourceChannel;
import org.apache.cockpit.common.spi.datasource.DataSourceChannelFactory;
import org.apache.cockpit.common.spi.enums.DbType;

@AutoService(DataSourceChannelFactory.class)
public class DB2DataSourceChannelFactory implements DataSourceChannelFactory {

    @Override
    public String getName() {
        return DbType.DB2.getName();
    }

    @Override
    public DataSourceChannel create() {
        return new DB2DataSourceChannel();
    }
}
