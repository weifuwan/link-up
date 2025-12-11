package org.apache.cockpit.connectors.api.jdbc.flow;

import com.typesafe.config.Config;
import lombok.Getter;
import lombok.Setter;
import org.apache.cockpit.common.log.Logger;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.CatalogTableUtil;
import org.apache.cockpit.connectors.api.catalog.TableIdentifier;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.common.metrics.MetricsContext;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.factory.FactoryUtil;
import org.apache.cockpit.connectors.api.factory.TableSourceFactory;
import org.apache.cockpit.connectors.api.jdbc.context.context.SourceReaderContext;
import org.apache.cockpit.connectors.api.source.*;
import org.apache.cockpit.connectors.api.type.SeaTunnelDataType;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;


public class SourceFlowLifeCycle<T, SplitT extends SourceSplit> implements FlowLifeCycle {

    private SourceReader<T, SplitT> reader;

    private SeaTunnelSourceCollector<T> collector;


    @Getter
    @Setter
    protected Boolean prepareClose;
    private CatalogTable catalogTable;

    protected final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    private SourceReader.Context context;
    private final SeaTunnelSource<SeaTunnelRow, SourceSplit> source;
    private final MetricsContext metricsContext;
    private final Logger logger;

    public SourceFlowLifeCycle(Config config, MetricsContext metricsContext, Logger logger) throws Exception {
        this.logger = logger;
        DbType pluginType = DbType.of(config.getString("plugin-type"));
        if (pluginType == null) {
            throw new RuntimeException("未找到对应的数据库类型");
        }
        Optional<TableSourceFactory> factory = FactoryUtil.discoverOptionalFactory(classLoader, TableSourceFactory.class, pluginType.getCode());
        if (!factory.isPresent()) {
            String logMessage = "没有找到对应的Factory";
            logger.log(logMessage);
            throw new RuntimeException(logMessage);
        }
        TableSourceFactory tableSourceFactory = factory.get();
        source = FactoryUtil.createAndPrepareSource(tableSourceFactory, ReadonlyConfig.fromConfig(config), classLoader);
        this.metricsContext = metricsContext;
        this.prepareClose = false;
    }

    public void init() throws Exception {
        this.context =
                new SourceReaderContext(
                        Boundedness.BOUNDED,
                        this,
                        metricsContext);
        this.reader = (SourceReader<T, SplitT>) source.<T, SplitT>createReader(context);
    }

    public void open() throws Exception {
        logger.log("打开资源");
        reader.open();
        catalogTable = this.reader.getJdbcSourceTables();
        if (catalogTable == null) {
            throw new RuntimeException("source catalog table is null");
        }
    }


    public void initCollector(Config config) throws Exception {
        SeaTunnelDataType sourceProducedType;
        List<TablePath> tablePaths = new ArrayList<>();
        try {
            List<CatalogTable> producedCatalogTables = source.getProducedCatalogTables();
            sourceProducedType = CatalogTableUtil.convertToDataType(producedCatalogTables);
            tablePaths =
                    producedCatalogTables.stream()
                            .map(CatalogTable::getTableId)
                            .map(TableIdentifier::toTablePath)
                            .collect(Collectors.toList());
        } catch (UnsupportedOperationException e) {
            // TODO remove it when all connector use `getProducedCatalogTables`
            sourceProducedType = source.getProducedType();
        }
        SinkFlowLifeCycle<T> sinkFlowLifeCycle = new SinkFlowLifeCycle<>(ReadonlyConfig.fromConfig(config), catalogTable,
                metricsContext, config.getString("plugin-type"));
        this.collector = new SeaTunnelSourceCollector<T>(sinkFlowLifeCycle, sourceProducedType, tablePaths, metricsContext, logger);

    }


    public void collect() throws Exception {
        if (!prepareClose) {

            reader.pollNext(collector);
            if (collector.isEmptyThisPollNext()) {
                Thread.sleep(100);
            } else {
                collector.resetEmptyThisPollNext();
                /*
                 * The current thread obtain a checkpoint lock in the method {@link
                 * SourceReader#pollNext(Collector)}. When trigger the checkpoint or savepoint,
                 * other threads try to obtain the lock in the method {@link
                 * SourceFlowLifeCycle#triggerBarrier(Barrier)}. When high CPU load, checkpoint
                 * process may be blocked as long time. So we need sleep to free the CPU.
                 */
                Thread.sleep(10L);
            }

        } else {
            Thread.sleep(100);
        }
    }

    public void close() throws Exception {
        reader.close();
        signalNoMoreElement();
        if (collector != null) {
            collector.close();
        }
        logger.log("关闭资源");
        logger.close();
    }

    public void signalNoMoreElement() {
        // ready close this reader
        try {
            this.prepareClose = true;
        } catch (Exception e) {
            logger.log("source close failed " + e);
            throw new RuntimeException(e);
        }
    }


}
