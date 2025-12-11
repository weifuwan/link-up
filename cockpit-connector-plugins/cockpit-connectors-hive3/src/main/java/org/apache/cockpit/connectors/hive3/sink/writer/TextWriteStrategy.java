package org.apache.cockpit.connectors.hive3.sink.writer;


import lombok.NonNull;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.exception.CommonError;
import org.apache.cockpit.connectors.api.catalog.exception.CommonErrorCodeDeprecated;
import org.apache.cockpit.connectors.api.serialization.SerializationSchema;
import org.apache.cockpit.connectors.api.serialization.text.TextSerializationSchema;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.util.DateTimeUtils;
import org.apache.cockpit.connectors.api.util.DateUtils;
import org.apache.cockpit.connectors.api.util.EncodingUtils;
import org.apache.cockpit.connectors.api.util.TimeUtils;
import org.apache.cockpit.connectors.hive3.config.FileFormat;
import org.apache.cockpit.connectors.hive3.config.FileSinkConfig;
import org.apache.cockpit.connectors.hive3.exception.FileConnectorException;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class TextWriteStrategy extends AbstractWriteStrategy<FSDataOutputStream> {
    private final LinkedHashMap<String, FSDataOutputStream> beingWrittenOutputStream;
    private final Map<String, Boolean> isFirstWrite;
    private final String fieldDelimiter;
    private final String rowDelimiter;
    private final DateUtils.Formatter dateFormat;
    private final DateTimeUtils.Formatter dateTimeFormat;
    private final TimeUtils.Formatter timeFormat;
    private final FileFormat fileFormat;
    private final Boolean enableHeaderWriter;
    private final Charset charset;
    private SerializationSchema serializationSchema;

    public TextWriteStrategy(FileSinkConfig fileSinkConfig) {
        super(fileSinkConfig);
        this.beingWrittenOutputStream = new LinkedHashMap<>();
        this.isFirstWrite = new HashMap<>();
        this.fieldDelimiter = fileSinkConfig.getFieldDelimiter();
        this.rowDelimiter = fileSinkConfig.getRowDelimiter();
        this.dateFormat = fileSinkConfig.getDateFormat();
        this.dateTimeFormat = fileSinkConfig.getDatetimeFormat();
        this.timeFormat = fileSinkConfig.getTimeFormat();
        this.fileFormat = fileSinkConfig.getFileFormat();
        this.enableHeaderWriter = fileSinkConfig.getEnableHeaderWriter();
        this.charset = EncodingUtils.tryParseCharset(fileSinkConfig.getEncoding());
    }

    @Override
    public void setCatalogTable(CatalogTable catalogTable) {
        super.setCatalogTable(catalogTable);
        this.serializationSchema =
                TextSerializationSchema.builder()
                        .seaTunnelRowType(
                                buildSchemaWithRowType(
                                        catalogTable.getSeaTunnelRowType(), sinkColumnsIndexInRow))
                        .delimiter(fieldDelimiter)
                        .dateFormatter(dateFormat)
                        .dateTimeFormatter(dateTimeFormat)
                        .timeFormatter(timeFormat)
                        .charset(charset)
                        .build();
    }

    @Override
    public void write(@NonNull SeaTunnelRow seaTunnelRow) {
        super.write(seaTunnelRow);
        String filePath = getOrCreateFilePathBeingWritten(seaTunnelRow);
        FSDataOutputStream fsDataOutputStream = getOrCreateOutputStream(filePath);
        try {
            if (isFirstWrite.get(filePath)) {
                isFirstWrite.put(filePath, false);
            } else {
                fsDataOutputStream.write(rowDelimiter.getBytes(charset));
            }
            fsDataOutputStream.write(
                    serializationSchema.serialize(
                            seaTunnelRow.copy(
                                    sinkColumnsIndexInRow.stream()
                                            .mapToInt(Integer::intValue)
                                            .toArray())));
        } catch (IOException e) {
            throw CommonError.fileOperationFailed("TextFile", "write", filePath, e);
        }
    }

    @Override
    public void finishAndCloseFile() {
        beingWrittenOutputStream.forEach(
                (key, value) -> {
                    try {
                        value.flush();
                    } catch (IOException e) {
                        throw new FileConnectorException(
                                CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                                String.format("Flush data to this file [%s] failed", key),
                                e);
                    } finally {
                        try {
                            value.close();
                        } catch (IOException e) {
                            log.error("error when close output stream {}", key, e);
                        }
                    }
                    needMoveFiles.put(key, getTargetLocation(key));
                });
        beingWrittenOutputStream.clear();
        isFirstWrite.clear();
    }

    @Override
    public FSDataOutputStream getOrCreateOutputStream(@NonNull String filePath) {
        FSDataOutputStream fsDataOutputStream = beingWrittenOutputStream.get(filePath);
        if (fsDataOutputStream == null) {
            try {
                fsDataOutputStream = hadoopFileSystemProxy.getOutputStream(filePath);
                enableWriteHeader(fsDataOutputStream);
                beingWrittenOutputStream.put(filePath, fsDataOutputStream);
                isFirstWrite.put(filePath, true);
            } catch (Exception e) {
                throw CommonError.fileOperationFailed("TextFile", "open", filePath, e);
            }
        }
        return fsDataOutputStream;
    }

    private void enableWriteHeader(FSDataOutputStream fsDataOutputStream) throws IOException {
        if (enableHeaderWriter) {
            fsDataOutputStream.write(
                    String.join(fieldDelimiter, seaTunnelRowType.getFieldNames()).getBytes());
            fsDataOutputStream.write(rowDelimiter.getBytes());
        }
    }
}
