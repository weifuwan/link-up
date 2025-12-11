package org.apache.cockpit.connectors.api.factory;


import org.apache.cockpit.connectors.api.connector.TableSink;

import java.util.Collections;
import java.util.List;

/**
 * This is an SPI interface, used to create {@link TableSink}. Each plugin need to have it own
 * implementation.
 *
 */
public interface TableSinkFactory extends Factory {

    /**
     * We will never use this method now. So gave a default implement and return null.
     *
     * @param context TableFactoryContext
     * @return return the sink created by this factory
     */
    default <IN> TableSink<IN> createSink(
            TableSinkFactoryContext context) {
        throw new UnsupportedOperationException(
                "The Factory has not been implemented and the deprecated Plugin will be used.");
    }

    @Deprecated
    default List<String> excludeTablePlaceholderReplaceKeys() {
        return Collections.emptyList();
    }
}
