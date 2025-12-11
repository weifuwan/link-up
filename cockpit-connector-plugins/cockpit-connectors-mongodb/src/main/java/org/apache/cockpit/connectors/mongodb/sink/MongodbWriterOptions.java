package org.apache.cockpit.connectors.mongodb.sink;

import lombok.Getter;

import java.io.Serializable;

@Getter
public class MongodbWriterOptions implements Serializable {

    private static final long serialVersionUID = 1;

    protected final String connectString;

    protected final String database;

    protected final String collection;

    protected final int flushSize;

    protected final long batchIntervalMs;

    protected final boolean upsertEnable;

    protected final String[] primaryKey;

    protected final int retryMax;

    protected final long retryInterval;

    protected final boolean transaction;

    public MongodbWriterOptions(
            String connectString,
            String database,
            String collection,
            int flushSize,
            long batchIntervalMs,
            boolean upsertEnable,
            String[] primaryKey,
            int retryMax,
            long retryInterval,
            boolean transaction) {
        this.connectString = connectString;
        this.database = database;
        this.collection = collection;
        this.flushSize = flushSize;
        this.batchIntervalMs = batchIntervalMs;
        this.upsertEnable = upsertEnable;
        this.primaryKey = primaryKey;
        this.retryMax = retryMax;
        this.retryInterval = retryInterval;
        this.transaction = transaction;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder For {@link MongodbWriterOptions}. */
    public static class Builder {
        protected String connectString;

        protected String database;

        protected String collection;

        protected int flushSize;

        protected long batchIntervalMs;

        protected boolean upsertEnable;

        protected String[] primaryKey;

        protected int retryMax;

        protected long retryInterval;

        protected boolean transaction;

        public Builder withConnectString(String connectString) {
            this.connectString = connectString;
            return this;
        }

        public Builder withDatabase(String database) {
            this.database = database;
            return this;
        }

        public Builder withCollection(String collection) {
            this.collection = collection;
            return this;
        }

        public Builder withFlushSize(int flushSize) {
            this.flushSize = flushSize;
            return this;
        }

        public Builder withBatchIntervalMs(Long batchIntervalMs) {
            this.batchIntervalMs = batchIntervalMs;
            return this;
        }

        public Builder withUpsertEnable(boolean upsertEnable) {
            this.upsertEnable = upsertEnable;
            return this;
        }

        public Builder withPrimaryKey(String[] primaryKey) {
            this.primaryKey = primaryKey;
            return this;
        }

        public Builder withRetryMax(int retryMax) {
            this.retryMax = retryMax;
            return this;
        }

        public Builder withRetryInterval(Long retryInterval) {
            this.retryInterval = retryInterval;
            return this;
        }

        public Builder withTransaction(boolean transaction) {
            this.transaction = transaction;
            return this;
        }

        public MongodbWriterOptions build() {
            return new MongodbWriterOptions(
                    connectString,
                    database,
                    collection,
                    flushSize,
                    batchIntervalMs,
                    upsertEnable,
                    primaryKey,
                    retryMax,
                    retryInterval,
                    transaction);
        }
    }
}
