package org.apache.cockpit.connectors.starrocks.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.TablePath;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

@Slf4j
public class SchemaUtils {

    private static final String MIN_VERSION_TABLE_CHANGE_COLUMN = "3.3.2";

    private SchemaUtils() {
    }


    /**
     * Check if the column exists in the table
     */
    public static boolean columnExists(Connection connection, TablePath tablePath, String column) {
        String selectColumnSQL =
                String.format(
                        "SELECT %s FROM %s WHERE 1 != 1",
                        quoteIdentifier(column), tablePath.getFullName());
        try (Statement statement = connection.createStatement()) {
            return statement.execute(selectColumnSQL);
        } catch (SQLException e) {
            log.debug("Column {} does not exist in table {}", column, tablePath.getFullName(), e);
            return false;
        }
    }

    public static String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }
}
