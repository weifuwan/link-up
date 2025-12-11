package org.apache.cockpit.connectors.api.jdbc.format;


import org.apache.cockpit.connectors.api.catalog.exception.CommonErrorCodeDeprecated;
import org.apache.cockpit.connectors.api.jdbc.config.JdbcConnectionConfig;
import org.apache.cockpit.connectors.api.jdbc.connection.JdbcConnectionProvider;
import org.apache.cockpit.connectors.api.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.cockpit.connectors.api.jdbc.exception.JdbcConnectorException;
import org.apache.cockpit.connectors.api.jdbc.executor.JdbcBatchStatementExecutor;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;


/**
 * A JDBC outputFormat
 */
public class JdbcOutputFormat<I, E extends JdbcBatchStatementExecutor<I>> implements Serializable {

    protected final JdbcConnectionProvider connectionProvider;

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(JdbcOutputFormat.class);

    private final JdbcConnectionConfig jdbcConnectionConfig;
    private final StatementExecutorFactory<E> statementExecutorFactory;

    private transient E jdbcStatementExecutor;
    private transient int batchCount = 0;
    private transient volatile boolean closed = false;
    private transient volatile Exception flushException;

    public JdbcOutputFormat(
            JdbcConnectionProvider connectionProvider,
            JdbcConnectionConfig jdbcConnectionConfig,
            StatementExecutorFactory<E> statementExecutorFactory) {
        this.connectionProvider = checkNotNull(connectionProvider);
        this.jdbcConnectionConfig = checkNotNull(jdbcConnectionConfig);
        this.statementExecutorFactory = checkNotNull(statementExecutorFactory);
    }

    /**
     * Connects to the target database and initializes the prepared statement.
     */
    public void open() throws IOException {
        try {
            connectionProvider.getOrEstablishConnection();
        } catch (Exception e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.CONNECT_DATABASE_FAILED,
                    "unable to open JDBC writer",
                    e);
        }
        jdbcStatementExecutor = createAndOpenStatementExecutor(statementExecutorFactory);
    }

    private E createAndOpenStatementExecutor(StatementExecutorFactory<E> statementExecutorFactory) {
        E exec = statementExecutorFactory.get();
        try {
            exec.prepareStatements(connectionProvider.getConnection());
        } catch (SQLException e) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                    "unable to open JDBC writer",
                    e);
        }
        return exec;
    }

    public void checkFlushException() {
        if (flushException != null) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                    "Writing records to JDBC failed.",
                    flushException);
        }
    }

    public final synchronized void writeRecord(I record) {
        checkFlushException();
        try {
            addToBatch(record);
            batchCount++;
            if (jdbcConnectionConfig.getBatchSize() > 0
                    && batchCount >= jdbcConnectionConfig.getBatchSize()) {
                flush();
            }
        } catch (Exception e) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                    "Writing records to JDBC failed.",
                    e);
        }
    }

    protected void addToBatch(I record) throws SQLException {
        jdbcStatementExecutor.addToBatch(record);
    }

    public synchronized void flush() throws IOException {
        if (flushException != null) {
            LOG.warn(
                    String.format(
                            "An exception occurred during the previous flush process %s, skipping this flush",
                            ExceptionUtils.getMessage(flushException)));
            return;
        }
        if (batchCount == 0) {
            LOG.debug("No data to flush.");
            return;
        }

        final int sleepMs = 1000;
        for (int i = 0; i <= jdbcConnectionConfig.getMaxRetries(); i++) {
            try {
                attemptFlush();
                batchCount = 0;
                break;
            } catch (SQLException e) {
                LOG.error("JDBC executeBatch error, retry times = {}", i, e);
                if (i >= jdbcConnectionConfig.getMaxRetries()) {
                    throw new JdbcConnectorException(
                            CommonErrorCodeDeprecated.FLUSH_DATA_FAILED, e);
                }
                try {
                    if (!connectionProvider.isConnectionValid()) {
                        updateExecutor(true);
                    }
                } catch (Exception exception) {
                    LOG.error(
                            "JDBC connection is not valid, and reestablish connection failed.",
                            exception);
                    throw new JdbcConnectorException(
                            JdbcConnectorErrorCode.CONNECT_DATABASE_FAILED,
                            "Reestablish JDBC connection failed",
                            exception);
                }
                try {
                    Thread.sleep(sleepMs * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new JdbcConnectorException(
                            CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                            "unable to flush; interrupted while doing another attempt",
                            e);
                }
            }
        }
    }

    protected void attemptFlush() throws SQLException {
        jdbcStatementExecutor.executeBatch();
    }

    /**
     * Executes prepared statement and closes all resources of this instance.
     */
    public synchronized void close() {
        if (!closed) {
            closed = true;

            if (batchCount > 0) {
                try {
                    flush();
                } catch (Exception e) {
                    LOG.warn("Writing records to JDBC failed.", e);
                    flushException =
                            new JdbcConnectorException(
                                    CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                                    "Writing records to JDBC failed.",
                                    e);
                }
            }

            try {
                if (jdbcStatementExecutor != null) {
                    jdbcStatementExecutor.closeStatements();
                }
            } catch (SQLException e) {
                LOG.warn("Close JDBC writer failed.", e);
            }
        }
        connectionProvider.closeConnection();
        checkFlushException();
    }

    public void updateExecutor(boolean reconnect) throws SQLException, ClassNotFoundException {
        try {
            jdbcStatementExecutor.closeStatements();
        } catch (SQLException e) {
            if (!reconnect) {
                throw e;
            }
            LOG.error("Close JDBC statement failed on reconnect.", e);
        }
        jdbcStatementExecutor.prepareStatements(
                reconnect
                        ? connectionProvider.reestablishConnection()
                        : connectionProvider.getConnection());
    }

    /**
     * A factory for creating {@link JdbcBatchStatementExecutor} instance.
     *
     * @param <T> The type of instance.
     */
    public interface StatementExecutorFactory<T extends JdbcBatchStatementExecutor<?>>
            extends Supplier<T>, Serializable {
    }
}
