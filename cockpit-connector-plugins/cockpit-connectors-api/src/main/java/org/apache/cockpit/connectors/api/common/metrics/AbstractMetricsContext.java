package org.apache.cockpit.connectors.api.common.metrics;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public abstract class AbstractMetricsContext implements MetricsContext, Serializable {

    private static final long serialVersionUID = 1L;

    protected final Map<String, Metric> metrics = new ConcurrentHashMap<>();

    @Override
    public Counter counter(String name) {
        if (metrics.containsKey(name)) {
            return (Counter) metrics.get(name);
        }
        return this.counter(name, new ThreadSafeCounter(name));
    }

    @Override
    public <C extends Counter> C counter(String name, C counter) {
        this.addMetric(name, counter);
        return counter;
    }

    @Override
    public Meter meter(String name) {
        if (metrics.containsKey(name)) {
            return (Meter) metrics.get(name);
        }
        return this.meter(name, new ThreadSafeQPSMeter(name));
    }

    @Override
    public <M extends Meter> M meter(String name, M meter) {
        this.addMetric(name, meter);
        return meter;
    }

    protected void addMetric(String name, Metric metric) {
        if (metric == null) {
            log.warn("Ignoring attempted add of a metric due to being null for name {}.", name);
        } else {
            synchronized (this) {
                Metric prior = this.metrics.put(name, metric);
                if (prior != null) {
                    this.metrics.put(name, prior);
                    log.warn(
                            "Name collision: MetricsContext already contains a Metric with the name '"
                                    + name
                                    + "'. Metric will not be reported.");
                }
            }
        }
    }

    @Override
    public String toString() {
        return "AbstractMetricsContext{" + "metrics=" + metrics + '}';
    }
    @Override
    public Map<String, Metric> getAllMetrics() {
        return metrics;
    }
}
