package org.apache.cockpit.connectors.console.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelException;
import org.apache.cockpit.connectors.api.common.sink.AbstractSinkWriter;
import org.apache.cockpit.connectors.api.sink.SinkWriter;
import org.apache.cockpit.connectors.api.type.SeaTunnelDataType;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.api.util.JsonUtils;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class ConsoleSinkWriter extends AbstractSinkWriter<SeaTunnelRow> {

    private SeaTunnelRowType seaTunnelRowType;
    private final AtomicLong rowCounter = new AtomicLong(0);
    private final SinkWriter.Context context;

    boolean isPrintData = true;
    int delayMs = 0;

    public ConsoleSinkWriter(
            SeaTunnelRowType seaTunnelRowType,
            SinkWriter.Context context,
            boolean isPrintData,
            int delayMs) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.context = context;
        this.isPrintData = isPrintData;
        this.delayMs = delayMs;
        log.info("output rowType: {}", fieldsInfo(seaTunnelRowType));
    }

    @Override
    public void write(SeaTunnelRow element) {
        String[] arr = new String[seaTunnelRowType.getTotalFields()];
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        Object[] fields = element.getFields();
        for (int i = 0; i < fieldTypes.length; i++) {
            arr[i] = fieldToString(fieldTypes[i], fields[i]);
        }
        if (isPrintData) {
            log.info(
                    " rowIndex={}:  SeaTunnelRow#tableId={} SeaTunnelRow#kind={} : {}",
                    rowCounter.incrementAndGet(),
                    element.getTableId(),
                    element.getRowKind(),
                    StringUtils.join(arr, ", "));
        }
        if (delayMs > 0) {
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SeaTunnelException(e);
            }
        }
    }

    @Override
    public void close() {
    }

    private String fieldsInfo(SeaTunnelRowType seaTunnelRowType) {
        String[] fieldsInfo = new String[seaTunnelRowType.getTotalFields()];
        for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
            fieldsInfo[i] =
                    String.format(
                            "%s<%s>",
                            seaTunnelRowType.getFieldName(i), seaTunnelRowType.getFieldType(i));
        }
        return StringUtils.join(fieldsInfo, ", ");
    }

    private String fieldToString(SeaTunnelDataType<?> type, Object value) {
        if (value == null) {
            return null;
        }
        switch (type.getSqlType()) {
            case ARRAY:
            case BYTES:
                List<String> arrayData = new ArrayList<>();
                for (int i = 0; i < Array.getLength(value); i++) {
                    arrayData.add(String.valueOf(Array.get(value, i)));
                }
                return arrayData.toString();
            case MAP:
                return JsonUtils.toJsonString(value);
            case ROW:
                List<String> rowData = new ArrayList<>();
                SeaTunnelRowType rowType = (SeaTunnelRowType) type;
                for (int i = 0; i < rowType.getTotalFields(); i++) {
                    rowData.add(
                            fieldToString(
                                    rowType.getFieldTypes()[i],
                                    ((SeaTunnelRow) value).getField(i)));
                }
                return rowData.toString();
            default:
                return String.valueOf(value);
        }
    }
}
