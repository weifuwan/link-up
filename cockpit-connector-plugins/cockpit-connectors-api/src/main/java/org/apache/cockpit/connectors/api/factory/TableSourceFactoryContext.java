package org.apache.cockpit.connectors.api.factory;

import lombok.Getter;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;

@Getter
public class TableSourceFactoryContext extends TableFactoryContext {

    public TableSourceFactoryContext(ReadonlyConfig options, ClassLoader classLoader) {
        super(options, classLoader);
    }
}
