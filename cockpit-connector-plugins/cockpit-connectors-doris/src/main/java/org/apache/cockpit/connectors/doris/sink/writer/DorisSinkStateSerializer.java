package org.apache.cockpit.connectors.doris.sink.writer;



import org.apache.cockpit.connectors.api.serialization.Serializer;

import java.io.*;

/** Serializer for DorisWriterState. */
public class DorisSinkStateSerializer implements Serializer<DorisSinkState> {
    @Override
    public byte[] serialize(DorisSinkState dorisSinkState) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(dorisSinkState.getLabelPrefix());
            out.writeLong(dorisSinkState.getCheckpointId());
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public DorisSinkState deserialize(byte[] serialized) throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                final DataInputStream in = new DataInputStream(bais)) {
            final String labelPrefix = in.readUTF();
            final long checkpointId = in.readLong();
            return new DorisSinkState(labelPrefix, checkpointId);
        }
    }
}
