package org.apache.cockpit.connectors.api.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.log.Logger;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelException;
import org.apache.cockpit.connectors.api.common.metrics.MetricsContext;
import org.apache.cockpit.connectors.api.common.metrics.TaskMetricsCalcContext;
import org.apache.cockpit.connectors.api.constant.PluginType;
import org.apache.cockpit.connectors.api.jdbc.flow.SinkFlowLifeCycle;
import org.apache.cockpit.connectors.api.type.MultipleRowType;
import org.apache.cockpit.connectors.api.type.SeaTunnelDataType;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.api.util.StringFormatUtils;
import org.apache.commons.collections.CollectionUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SeaTunnelSourceCollector<T> implements Collector<T> {

    private final SinkFlowLifeCycle<T> sinkFlowLifeCycle;
    private final Object checkpoint;
    private final TaskMetricsCalcContext taskMetricsCalcContext;
    private final SeaTunnelDataType<T> rowType;
    private final Map<String, SeaTunnelRowType> rowTypeMap = new HashMap<>();

    private final ScheduledExecutorService monitorService;
    private final Logger logger;

    public SeaTunnelSourceCollector(SinkFlowLifeCycle<T> sinkFlowLifeCycle, SeaTunnelDataType<T> rowType,
                                    List<TablePath> tablePaths, MetricsContext metricsContext,
                                    Logger logger) {
        this.logger = logger;
        monitorService = Executors.newSingleThreadScheduledExecutor();
        monitorService.scheduleAtFixedRate(
                this::printExecutionInfo,
                1,      // 初始延迟 1 秒
                10,      // 每 2 秒执行一次
                TimeUnit.SECONDS);
        this.sinkFlowLifeCycle = sinkFlowLifeCycle;
        this.checkpoint = new Object();
        this.rowType = rowType;

        if (rowType instanceof MultipleRowType) {
            ((MultipleRowType) rowType)
                    .iterator()
                    .forEachRemaining(type -> this.rowTypeMap.put(type.getKey(), type.getValue()));
        }
        this.taskMetricsCalcContext =
                new TaskMetricsCalcContext(
                        metricsContext,
                        PluginType.SOURCE,
                        CollectionUtils.isNotEmpty(tablePaths),
                        tablePaths);

    }


    private void printExecutionInfo() {

        logger.log(
                StringFormatUtils.formatTable(
                        "Source任务状态",
                        "count",
                        taskMetricsCalcContext.getCount().getCount(),
                        "bytes",
                        taskMetricsCalcContext.getBytes().getCount(),
                        "qps",
                        taskMetricsCalcContext.getQPS().getRate(),
                        "bytesPerSeconds",
                        taskMetricsCalcContext.getBytesPerSeconds().getRate()
                ));
    }

    @Override
    public void collect(T row) throws Exception {
        try {
            if (row instanceof SeaTunnelRow) {
                String tableId = ((SeaTunnelRow) row).getTableId();
                int size;
                if (rowType instanceof SeaTunnelRowType) {
                    size = ((SeaTunnelRow) row).getBytesSize((SeaTunnelRowType) rowType);
                } else if (rowType instanceof MultipleRowType) {
                    size = ((SeaTunnelRow) row).getBytesSize(rowTypeMap.get(tableId));
                } else {
                    throw new SeaTunnelException(
                            "Unsupported row type: " + rowType.getClass().getName());
                }
                taskMetricsCalcContext.updateMetrics(row, tableId);
            }
            sendRecordToNext(row);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object getCheckpointLock() {
        return checkpoint;
    }

    public void sendRecordToNext(T row) throws IOException {
        sinkFlowLifeCycle.received(row);
    }

    @Override
    public void close() throws Exception {
        printExecutionInfo();
        monitorService.shutdown();
        sinkFlowLifeCycle.close();
    }
}
