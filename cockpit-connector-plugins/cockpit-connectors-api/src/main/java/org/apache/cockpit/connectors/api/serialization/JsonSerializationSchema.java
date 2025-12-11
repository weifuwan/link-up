package org.apache.cockpit.connectors.api.serialization;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Getter;
import org.apache.cockpit.connectors.api.catalog.exception.CommonError;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static com.google.common.base.Preconditions.checkNotNull;

public class JsonSerializationSchema implements SerializationSchema {

    public static final String FORMAT = "Common";
    /** RowType to generate the runtime converter. */
    private final SeaTunnelRowType rowType;

    /** Reusable object node. */
    private transient ObjectNode node;

    /** Object mapper that is used to create output JSON objects. */
    @Getter private final ObjectMapper mapper = new ObjectMapper();

    private final Charset charset;

    private final RowToJsonConverters.RowToJsonConverter runtimeConverter;

    public JsonSerializationSchema(SeaTunnelRowType rowType) {
        this(rowType, StandardCharsets.UTF_8);
    }

    public JsonSerializationSchema(SeaTunnelRowType rowType, Charset charset) {
        this.rowType = rowType;
        this.runtimeConverter = new RowToJsonConverters().createConverter(checkNotNull(rowType));
        this.charset = charset;
    }

    public JsonSerializationSchema(SeaTunnelRowType rowType, String nullValue) {
        this.rowType = rowType;
        this.runtimeConverter =
                new RowToJsonConverters().createConverter(checkNotNull(rowType), nullValue);
        this.charset = StandardCharsets.UTF_8;
    }

    {
        mapper.configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
    }

    @Override
    public byte[] serialize(SeaTunnelRow row) {
        if (node == null) {
            node = mapper.createObjectNode();
        }

        try {
            runtimeConverter.convert(mapper, node, row);
            return mapper.writeValueAsString(node).getBytes(charset);
        } catch (Throwable t) {
            throw CommonError.jsonOperationError(FORMAT, row.toString(), t);
        }
    }
}
