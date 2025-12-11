package org.apache.cockpit.connectors.api.source;


/**
 * A {@link Collector} is used to collect data from {@link SourceReader}.
 *
 * @param <T> data type.
 */
public interface Collector<T> {

    void collect(T record) throws Exception;

    void close() throws Exception;

    /**
     * Returns the checkpoint lock.
     *
     * @return The object to use as the lock
     */
    Object getCheckpointLock();

    default boolean isEmptyThisPollNext() {
        return false;
    }

    default void resetEmptyThisPollNext() {
    }
}
