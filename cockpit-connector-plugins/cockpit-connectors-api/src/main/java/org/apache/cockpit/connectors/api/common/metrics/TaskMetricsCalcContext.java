package org.apache.cockpit.connectors.api.common.metrics;

import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.constant.PluginType;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.cockpit.connectors.api.common.metrics.MetricNames.*;


public class TaskMetricsCalcContext {

    private final MetricsContext metricsContext;

    private final PluginType type;

    private Counter count;

    private Map<String, Counter> countPerTable = new ConcurrentHashMap<>();



    private Meter QPS;

    private Map<String, Meter> QPSPerTable = new ConcurrentHashMap<>();

    private Counter bytes;

    private Map<String, Counter> bytesPerTable = new ConcurrentHashMap<>();

    private Meter bytesPerSeconds;

    private Map<String, Meter> bytesPerSecondsPerTable = new ConcurrentHashMap<>();


    public Counter getCount() {
        return count;
    }

    public Meter getQPS() {
        return QPS;
    }

    public Counter getBytes() {
        return bytes;
    }

    public Meter getBytesPerSeconds() {
        return bytesPerSeconds;
    }

    public TaskMetricsCalcContext(
            MetricsContext metricsContext,
            PluginType type,
            boolean isMulti,
            List<TablePath> tables) {
        this.metricsContext = metricsContext;
        this.type = type;
        initializeMetrics(isMulti, tables);
    }

    private void initializeMetrics(boolean isMulti, List<TablePath> tables) {
        if (type.equals(PluginType.SINK)) {
            this.initializeMetrics(
                    isMulti,
                    tables,
                    SINK_WRITE_COUNT,
                    SINK_WRITE_QPS,
                    SINK_WRITE_BYTES,
                    SINK_WRITE_BYTES_PER_SECONDS);
        } else if (type.equals(PluginType.SOURCE)) {
            this.initializeMetrics(
                    isMulti,
                    tables,
                    SOURCE_RECEIVED_COUNT,
                    SOURCE_RECEIVED_QPS,
                    SOURCE_RECEIVED_BYTES,
                    SOURCE_RECEIVED_BYTES_PER_SECONDS);
        }
    }

    private void initializeMetrics(
            boolean isMulti,
            List<TablePath> tables,
            String countName,
            String qpsName,
            String bytesName,
            String bytesPerSecondsName) {
        count = metricsContext.counter(countName);
        QPS = metricsContext.meter(qpsName);
        bytes = metricsContext.counter(bytesName);
        bytesPerSeconds = metricsContext.meter(bytesPerSecondsName);
        if (isMulti) {
            tables.forEach(
                    tablePath -> {
                        countPerTable.put(
                                tablePath.getFullName(),
                                metricsContext.counter(countName + "#" + tablePath.getFullName()));
                        QPSPerTable.put(
                                tablePath.getFullName(),
                                metricsContext.meter(qpsName + "#" + tablePath.getFullName()));
                        bytesPerTable.put(
                                tablePath.getFullName(),
                                metricsContext.counter(bytesName + "#" + tablePath.getFullName()));
                        bytesPerSecondsPerTable.put(
                                tablePath.getFullName(),
                                metricsContext.meter(
                                        bytesPerSecondsName + "#" + tablePath.getFullName()));
                    });
        }
    }

    public void updateMetrics(Object data, String tableId) {
        count.inc();
        QPS.markEvent();
        if (data instanceof SeaTunnelRow) {
            SeaTunnelRow row = (SeaTunnelRow) data;
            bytes.inc(row.getBytesSize());
            bytesPerSeconds.markEvent(row.getBytesSize());

            if (StringUtils.isNotBlank(tableId)) {
                String tableName = TablePath.of(tableId).getFullName();

                // Processing count
                processMetrics(
                        countPerTable,
                        Counter.class,
                        tableName,
                        SINK_WRITE_COUNT,
                        SOURCE_RECEIVED_COUNT,
                        Counter::inc);

                // Processing bytes
                processMetrics(
                        bytesPerTable,
                        Counter.class,
                        tableName,
                        SINK_WRITE_BYTES,
                        SOURCE_RECEIVED_BYTES,
                        counter -> counter.inc(row.getBytesSize()));

                // Processing QPS
                processMetrics(
                        QPSPerTable,
                        Meter.class,
                        tableName,
                        SINK_WRITE_QPS,
                        SOURCE_RECEIVED_QPS,
                        Meter::markEvent);

                // Processing bytes rate
                processMetrics(
                        bytesPerSecondsPerTable,
                        Meter.class,
                        tableName,
                        SINK_WRITE_BYTES_PER_SECONDS,
                        SOURCE_RECEIVED_BYTES_PER_SECONDS,
                        meter -> meter.markEvent(row.getBytesSize()));
            }
        }
    }

    private <T> void processMetrics(
            Map<String, T> metricMap,
            Class<T> cls,
            String tableName,
            String sinkMetric,
            String sourceMetric,
            MetricProcessor<T> processor) {
        T metric = metricMap.get(tableName);
        if (Objects.nonNull(metric)) {
            processor.process(metric);
        } else {
            String metricName =
                    PluginType.SINK.equals(type)
                            ? sinkMetric + "#" + tableName
                            : sourceMetric + "#" + tableName;
            T newMetric = createMetric(metricsContext, metricName, cls);
            processor.process(newMetric);
            metricMap.put(tableName, newMetric);
        }
    }

    private <T> T createMetric(
            MetricsContext metricsContext, String metricName, Class<T> metricClass) {
        if (metricClass == Counter.class) {
            return metricClass.cast(metricsContext.counter(metricName));
        } else if (metricClass == Meter.class) {
            return metricClass.cast(metricsContext.meter(metricName));
        }
        throw new IllegalArgumentException("Unsupported metric class: " + metricClass.getName());
    }

    @FunctionalInterface
    interface MetricProcessor<T> {
        void process(T t);
    }
}
