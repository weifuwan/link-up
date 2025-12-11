
package org.apache.cockpit.common.spi.datasource;

import org.apache.cockpit.common.spi.plugin.PrioritySPI;
import org.apache.cockpit.common.spi.plugin.SPIIdentify;

public interface DataSourceChannelFactory extends PrioritySPI {

    /**
     * get datasource client
     */
    DataSourceChannel create();

    /**
     * get registry component name
     */
    String getName();

    @Override
    default SPIIdentify getIdentify() {
        return SPIIdentify.builder().name(getName()).build();
    }
}
