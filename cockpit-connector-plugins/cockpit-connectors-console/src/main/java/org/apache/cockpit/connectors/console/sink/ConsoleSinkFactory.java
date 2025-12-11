package org.apache.cockpit.connectors.console.sink;

import com.google.auto.service.AutoService;
import org.apache.cockpit.connectors.api.config.OptionRule;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.connector.TableSink;
import org.apache.cockpit.connectors.api.factory.Factory;
import org.apache.cockpit.connectors.api.factory.TableSinkFactory;
import org.apache.cockpit.connectors.api.factory.TableSinkFactoryContext;

@AutoService(Factory.class)
public class ConsoleSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return "CONSOLE";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .optional(
                        ConsoleSinkOptions.LOG_PRINT_DATA,
                        ConsoleSinkOptions.LOG_PRINT_DELAY,
                        ConsoleSinkOptions.MULTI_TABLE_SINK_REPLICA)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig options = context.getOptions();
        return () -> new ConsoleSink(context.getCatalogTable(), options);
    }
}
