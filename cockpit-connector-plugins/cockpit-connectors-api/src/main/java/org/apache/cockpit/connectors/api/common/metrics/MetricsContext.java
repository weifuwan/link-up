package org.apache.cockpit.connectors.api.common.metrics;

import java.util.Map;

public interface MetricsContext {

    /**
     * registers a {@link ThreadSafeCounter} with SeaTunnel.
     *
     * @param name name of the counter
     * @return the created counter
     */
    Counter counter(String name);

    /**
     * Registers a {@link Counter} with SeaTunnel.
     *
     * @param name name of the counter
     * @param counter counter to register
     * @param <C> counter type
     * @return the given counter
     */
    <C extends Counter> C counter(String name, C counter);

    /**
     * Registers a {@link ThreadSafeQPSMeter} with SeaTunnel.
     *
     * @param name name of the meter
     * @return the registered meter
     */
    Meter meter(String name);

    /**
     * Registers a new {@link Meter} with SeaTunnel.
     *
     * @param name name of the meter
     * @param meter meter to register
     * @param <M> meter type
     * @return the registered meter
     */
    <M extends Meter> M meter(String name, M meter);


    Map<String, Metric> getAllMetrics();
}
