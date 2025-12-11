package org.apache.cockpit.connectors.mongodb.source.config;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.cockpit.connectors.mongodb.config.MongodbConfig.*;


/** The configuration class for MongoDB source. */
@EqualsAndHashCode
@Getter
public class MongodbReadOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int fetchSize;

    private final boolean noCursorTimeout;

    private final long maxTimeMS;

    private MongodbReadOptions(int fetchSize, boolean noCursorTimeout, long maxTimeMS) {
        this.fetchSize = fetchSize;
        this.noCursorTimeout = noCursorTimeout;
        this.maxTimeMS = maxTimeMS;
    }

    public static MongoReadOptionsBuilder builder() {
        return new MongoReadOptionsBuilder();
    }

    /** Builder for {@link MongodbReadOptions}. */
    public static class MongoReadOptionsBuilder {

        private int fetchSize = FETCH_SIZE.defaultValue();

        private boolean noCursorTimeout = CURSOR_NO_TIMEOUT.defaultValue();

        private long maxTimeMin = MAX_TIME_MIN.defaultValue();

        private MongoReadOptionsBuilder() {}

        public MongoReadOptionsBuilder setFetchSize(int fetchSize) {
            checkArgument(fetchSize > 0, "The fetch size must be larger than 0.");
            this.fetchSize = fetchSize;
            return this;
        }

        public MongoReadOptionsBuilder setNoCursorTimeout(boolean noCursorTimeout) {
            this.noCursorTimeout = noCursorTimeout;
            return this;
        }

        public MongoReadOptionsBuilder setMaxTimeMS(long maxTimeMS) {
            this.maxTimeMin = maxTimeMS;
            return this;
        }

        public MongodbReadOptions build() {
            return new MongodbReadOptions(fetchSize, noCursorTimeout, maxTimeMin);
        }
    }
}
