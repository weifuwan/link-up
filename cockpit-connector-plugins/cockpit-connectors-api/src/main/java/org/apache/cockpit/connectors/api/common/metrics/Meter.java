package org.apache.cockpit.connectors.api.common.metrics;

/** Metric for measuring throughput. */
public interface Meter extends Metric {
    /** Mark occurrence of an event. */
    void markEvent();

    /**
     * Mark occurrence of multiple events.
     *
     * @param n number of events occurred
     */
    void markEvent(long n);

    /**
     * Returns the current rate of events per second.
     *
     * @return current rate of events per second
     */
    double getRate();

    /**
     * Get number of events marked on the meter.
     *
     * @return number of events marked on the meter
     */
    long getCount();
}
