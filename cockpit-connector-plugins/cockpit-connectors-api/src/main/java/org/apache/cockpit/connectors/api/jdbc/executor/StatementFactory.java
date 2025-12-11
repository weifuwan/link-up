package org.apache.cockpit.connectors.api.jdbc.executor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@FunctionalInterface
public interface StatementFactory {

    PreparedStatement createStatement(Connection connection) throws SQLException;
}
