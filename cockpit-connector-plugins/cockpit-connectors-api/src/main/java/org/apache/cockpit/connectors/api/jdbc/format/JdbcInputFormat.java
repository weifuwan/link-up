package org.apache.cockpit.connectors.api.jdbc.format;


import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.catalog.TableSchema;
import org.apache.cockpit.connectors.api.catalog.exception.CommonErrorCodeDeprecated;
import org.apache.cockpit.connectors.api.jdbc.config.JdbcSourceConfig;
import org.apache.cockpit.connectors.api.jdbc.converter.JdbcRowConverter;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialect;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialectLoader;
import org.apache.cockpit.connectors.api.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.cockpit.connectors.api.jdbc.exception.JdbcConnectorException;
import org.apache.cockpit.connectors.api.jdbc.source.ChunkSplitter;
import org.apache.cockpit.connectors.api.jdbc.source.JdbcSourceSplit;
import org.apache.cockpit.connectors.api.type.RowKind;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * InputFormat to read data from a database and generate Rows. The InputFormat has to be configured
 * using the supplied InputFormatBuilder. A valid RowTypeInfo must be properly configured in the
 * builder
 */
public class JdbcInputFormat implements Serializable {

    private static final long serialVersionUID = 2L;
    private static final Logger LOG = LoggerFactory.getLogger(JdbcInputFormat.class);


    private final JdbcDialect jdbcDialect;
    private final JdbcRowConverter jdbcRowConverter;
    private final Map<TablePath, CatalogTable> tables;
    private final ChunkSplitter chunkSplitter;

    private transient String splitTableId;
    private transient TableSchema splitTableSchema;
    private transient PreparedStatement statement;
    private transient ResultSet resultSet;
    private volatile boolean hasNext;

    public JdbcInputFormat(JdbcSourceConfig config, Map<TablePath, CatalogTable> tables, ChunkSplitter chunkSplitter) {
        this.jdbcDialect =
                JdbcDialectLoader.load(
                        config.getJdbcConnectionConfig().getJdbcUrl(),
                        config.getJdbcConnectionConfig().getDialect(),
                        config.getCompatibleMode());
        this.chunkSplitter = chunkSplitter;
        this.jdbcRowConverter = jdbcDialect.getRowConverter();
        this.tables = tables;
    }

    public void openInputFormat() {}

    public void closeInputFormat() throws IOException {
        close();

        if (chunkSplitter != null) {
            chunkSplitter.close();
        }
    }

    /**
     * Connects to the source database and executes the query
     *
     * @param inputSplit which is ignored if this InputFormat is executed as a non-parallel source,
     *     a "hook" to the query parameters otherwise (using its <i>parameterId</i>)
     * @throws IOException if there's an error during the execution of the query
     */
    public void open(JdbcSourceSplit inputSplit) throws IOException {
        try {
            splitTableSchema = tables.get(inputSplit.getTablePath()).getTableSchema();
            splitTableId = inputSplit.getTablePath().toString();

            statement = chunkSplitter.generateSplitStatement(inputSplit, splitTableSchema);
            resultSet = statement.executeQuery();
            hasNext = resultSet.next();
        } catch (SQLException se) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.CONNECT_DATABASE_FAILED,
                    "open() failed." + se.getMessage(),
                    se);
        }
    }

    /**
     * Closes all resources used.
     *
     * @throws IOException Indicates that a resource could not be closed.
     */
    public void close() throws IOException {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                LOG.info("ResultSet couldn't be closed - " + e.getMessage());
            }
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                LOG.info("Statement couldn't be closed - " + e.getMessage());
            }
        }
    }

    /**
     * Checks whether all data has been read.
     *
     * @return boolean value indication whether all data has been read.
     */
    public boolean reachedEnd() {
        return !hasNext;
    }

    /** Convert a row of data to seatunnelRow */
    public SeaTunnelRow nextRecord() {
        try {
            if (!hasNext) {
                return null;
            }
            SeaTunnelRow seaTunnelRow = jdbcRowConverter.toInternal(resultSet, splitTableSchema);
            seaTunnelRow.setTableId(splitTableId);
            seaTunnelRow.setRowKind(RowKind.INSERT);

            // update hasNext after we've read the record
            hasNext = resultSet.next();
            return seaTunnelRow;
        } catch (SQLException se) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                    "Couldn't read data - " + se.getMessage(),
                    se);
        } catch (NullPointerException npe) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                    "Couldn't access resultSet",
                    npe);
        }
    }
}
