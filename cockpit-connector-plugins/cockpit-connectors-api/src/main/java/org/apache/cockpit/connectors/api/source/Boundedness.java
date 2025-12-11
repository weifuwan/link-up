package org.apache.cockpit.connectors.api.source;

/**
 * Used to define the boundedness of a source. In batch mode, the source is {@link
 * Boundedness#BOUNDED}. In streaming mode, the source is {@link Boundedness#UNBOUNDED}.
 */
public enum Boundedness {
    /** A BOUNDED stream is a stream with finite records. */
    BOUNDED,
    /** A UNBOUNDED stream is a stream with infinite records. */
    UNBOUNDED
}
