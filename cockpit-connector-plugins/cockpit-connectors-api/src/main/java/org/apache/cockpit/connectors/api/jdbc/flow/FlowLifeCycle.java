package org.apache.cockpit.connectors.api.jdbc.flow;

import java.io.IOException;

public interface FlowLifeCycle {

    default void init() throws Exception {}

    default void open() throws Exception {}

    default void close() throws Exception {}

    default void prepareClose() throws IOException {}
}
