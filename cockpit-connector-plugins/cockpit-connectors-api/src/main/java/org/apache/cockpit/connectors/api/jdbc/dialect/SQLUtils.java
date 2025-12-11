package org.apache.cockpit.connectors.api.jdbc.dialect;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@Slf4j
public class SQLUtils {

    public static Long countForSubquery(Connection connection, String subQuerySQL)
            throws SQLException {
        String sqlQuery = String.format("SELECT COUNT(*) FROM (%s) T", subQuerySQL);
        log.info("Split Chunk, countForSubquery: {}", sqlQuery);
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet resultSet = stmt.executeQuery(sqlQuery)) {
                if (resultSet.next()) {
                    return resultSet.getLong(1);
                }
                throw new SQLException(
                        String.format("No result returned after running query [%s]", sqlQuery));
            }
        }
    }

    public static Long countForTable(Connection connection, String tablePath) throws SQLException {
        String sqlQuery = String.format("SELECT COUNT(*) FROM %s", tablePath);
        log.info("Split Chunk, countForTable: {}", sqlQuery);
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet resultSet = stmt.executeQuery(sqlQuery)) {
                if (resultSet.next()) {
                    return resultSet.getLong(1);
                }
                throw new SQLException(
                        String.format("No result returned after running query [%s]", sqlQuery));
            }
        }
    }
}
