package org.apache.cockpit.connectors.api.jdbc.flow;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelRuntimeException;
import org.apache.cockpit.connectors.api.common.metrics.MetricsContext;
import org.apache.cockpit.connectors.api.common.metrics.TaskMetricsCalcContext;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.constant.PluginType;
import org.apache.cockpit.connectors.api.factory.FactoryUtil;
import org.apache.cockpit.connectors.api.factory.TableSinkFactory;
import org.apache.cockpit.connectors.api.jdbc.context.context.SinkWriterContext;
import org.apache.cockpit.connectors.api.jdbc.sink.SupportSaveMode;
import org.apache.cockpit.connectors.api.sink.SaveModeExecuteWrapper;
import org.apache.cockpit.connectors.api.sink.SaveModeHandler;
import org.apache.cockpit.connectors.api.sink.SeaTunnelSink;
import org.apache.cockpit.connectors.api.sink.SinkWriter;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.util.StringFormatUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelAPIErrorCode.HANDLE_SAVE_MODE_FAILED;

@Slf4j
public class SinkFlowLifeCycle<T> implements FlowLifeCycle {
    protected final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    private final SinkWriter<T> writer;
    private final SinkWriter.Context context;
    private final TaskMetricsCalcContext taskMetricsCalcContext;
    @Getter
    @Setter
    protected Boolean prepareClose;

    private final ScheduledExecutorService monitorService;

    private final SeaTunnelSink<SeaTunnelRow> sink;

    public SinkFlowLifeCycle(ReadonlyConfig config, CatalogTable catalogTable, MetricsContext metricsContext, String pluginType) throws Exception {
        monitorService = Executors.newSingleThreadScheduledExecutor();
        monitorService.scheduleAtFixedRate(
                this::printExecutionInfo,
                1,      // 初始延迟 1 秒
                10,      // 每 2 秒执行一次
                TimeUnit.SECONDS);
        Optional<TableSinkFactory> factory = FactoryUtil.discoverOptionalFactory(classLoader, TableSinkFactory.class, DbType.valueOf(pluginType).getCode());
        if (!factory.isPresent()) {
            throw new RuntimeException("没有找到对应的Sink Factory");
        }
        this.prepareClose = false;
        TableSinkFactory tableSinkFactory = factory.get();
        this.sink = FactoryUtil.createAndPrepareSink(catalogTable, config, classLoader, DbType.valueOf(pluginType).getCode(), tableSinkFactory);
        handleSaveMode(sink);
        sink.open();
        this.context =
                new SinkWriterContext();
        try {
            this.writer = (SinkWriter<T>) sink.<T>createWriter(context);
        } catch (Exception e) {
            log.info("init sink failed");
            throw new RuntimeException("init sink failed", e);
        }


        List<TablePath> sinkTables = new ArrayList<>();


        Optional<CatalogTable> catalogTable1 = sink.getWriteCatalogTable();
        if (catalogTable1.isPresent()) {
            sinkTables.add(catalogTable1.get().getTablePath());
        } else {
            sinkTables.add(TablePath.DEFAULT);
        }

        this.taskMetricsCalcContext =
                new TaskMetricsCalcContext(metricsContext, PluginType.SINK, false, sinkTables);

    }


    public void received(T row) throws IOException {
        if (prepareClose) {
            return;
        }
        writer.write(row);



        Optional<CatalogTable> writeCatalogTable =
                sink.getWriteCatalogTable();
        String tableId =
                writeCatalogTable
                        .map(
                                catalogTable ->
                                        catalogTable.getTablePath().getFullName())
                        .orElseGet(TablePath.DEFAULT::getFullName);

        taskMetricsCalcContext.updateMetrics(row, tableId);
    }

    public void close() throws Exception {
        printExecutionInfo();
        monitorService.shutdown();
        writer.close();
    }

    public void handleSaveMode(SeaTunnelSink seaTunnelSink) {
        if (seaTunnelSink instanceof SupportSaveMode) {
            SupportSaveMode saveModeSink = (SupportSaveMode) seaTunnelSink;
            Optional<SaveModeHandler> saveModeHandler = saveModeSink.getSaveModeHandler();
            if (saveModeHandler.isPresent()) {
                try (SaveModeHandler handler = saveModeHandler.get()) {
                    handler.open();
                    new SaveModeExecuteWrapper(handler).execute();
                } catch (Exception e) {
                    throw new SeaTunnelRuntimeException(HANDLE_SAVE_MODE_FAILED, e);
                }
            }
        }
    }

    private void printExecutionInfo() {

        log.info(
                StringFormatUtils.formatTable(
                        "Sink任务状态",
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

}
