package org.apache.cockpit.connectors.doris.serialize;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cockpit.connectors.api.serialization.JsonSerializationSchema;
import org.apache.cockpit.connectors.api.serialization.SerializationSchema;
import org.apache.cockpit.connectors.api.serialization.text.TextSerializationSchema;
import org.apache.cockpit.connectors.api.type.RowKind;
import org.apache.cockpit.connectors.api.type.SeaTunnelDataType;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.doris.sink.writer.LoadConstants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.cockpit.connectors.api.type.BasicType.STRING_TYPE;
import static org.apache.cockpit.connectors.doris.sink.writer.LoadConstants.NULL_VALUE;


public class SeaTunnelRowSerializer implements DorisSerializer {
    public static final String JSON = "json";
    public static final String CSV = "csv";
    String type;
    private final SeaTunnelRowType seaTunnelRowType;
    private final String fieldDelimiter;
    private final boolean enableDelete;
    private final SerializationSchema serialize;
    private final boolean caseSensitive;

    public SeaTunnelRowSerializer(
            String type,
            SeaTunnelRowType seaTunnelRowType,
            String fieldDelimiter,
            boolean enableDelete) {
        this(type, seaTunnelRowType, fieldDelimiter, enableDelete, true);
    }

    public SeaTunnelRowSerializer(
            String type,
            SeaTunnelRowType seaTunnelRowType,
            String fieldDelimiter,
            boolean enableDelete,
            boolean caseSensitive) {
        this.type = type;
        this.fieldDelimiter = fieldDelimiter;
        this.enableDelete = enableDelete;
        this.caseSensitive = caseSensitive;

        String[] fieldNames = seaTunnelRowType.getFieldNames();
        String[] processedFieldNames = new String[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            processedFieldNames[i] = caseSensitive ? fieldNames[i] : fieldNames[i].toLowerCase();
        }

        List<Object> fieldNamesList = new ArrayList<>(Arrays.asList(processedFieldNames));
        List<SeaTunnelDataType<?>> fieldTypes =
                new ArrayList<>(Arrays.asList(seaTunnelRowType.getFieldTypes()));

        if (enableDelete) {
            fieldNamesList.add(LoadConstants.DORIS_DELETE_SIGN);
            fieldTypes.add(STRING_TYPE);
        }

        this.seaTunnelRowType =
                new SeaTunnelRowType(
                        fieldNamesList.toArray(new String[0]),
                        fieldTypes.toArray(new SeaTunnelDataType<?>[0]));

        if ("json".equals(type)) {
            JsonSerializationSchema jsonSerializationSchema =
                    new JsonSerializationSchema(this.seaTunnelRowType);
            ObjectMapper mapper = jsonSerializationSchema.getMapper();
            mapper.configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
            this.serialize = jsonSerializationSchema;
        } else {
            this.serialize =
                    TextSerializationSchema.builder()
                            .seaTunnelRowType(this.seaTunnelRowType)
                            .delimiter(fieldDelimiter)
                            .nullValue(NULL_VALUE)
                            .build();
        }
    }

    public byte[] buildJsonString(SeaTunnelRow row) {

        return serialize.serialize(row);
    }

    public byte[] buildCSVString(SeaTunnelRow row) {

        return serialize.serialize(row);
    }

    public String parseDeleteSign(RowKind rowKind) {
        if (RowKind.INSERT.equals(rowKind) || RowKind.UPDATE_AFTER.equals(rowKind)) {
            return "0";
        } else if (RowKind.DELETE.equals(rowKind) || RowKind.UPDATE_BEFORE.equals(rowKind)) {
            return "1";
        } else {
            throw new IllegalArgumentException("Unrecognized row kind:" + rowKind.toString());
        }
    }

    @Override
    public void open() throws IOException {
    }

    @Override
    public byte[] serialize(SeaTunnelRow seaTunnelRow) throws IOException {

        if (enableDelete) {

            List<Object> newFields = new ArrayList<>(Arrays.asList(seaTunnelRow.getFields()));
            newFields.add(parseDeleteSign(seaTunnelRow.getRowKind()));
            seaTunnelRow = new SeaTunnelRow(newFields.toArray());
        }

        if (JSON.equals(type)) {
            return buildJsonString(seaTunnelRow);
        } else if (CSV.equals(type)) {
            return buildCSVString(seaTunnelRow);
        } else {
            throw new IllegalArgumentException("The type " + type + " is not supported!");
        }
    }

    @Override
    public void close() throws IOException {
    }
}
