package org.apache.cockpit.connectors.clickhouse.sink.client.executor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@FunctionalInterface
public interface StatementFactory {

    PreparedStatement createStatement(Connection connection) throws SQLException;
}
