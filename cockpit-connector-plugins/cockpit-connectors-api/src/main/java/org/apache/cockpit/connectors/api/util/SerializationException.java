package org.apache.cockpit.connectors.api.util;

public class SerializationException extends RuntimeException {

    /**
     * Required for serialization support.
     *
     * @see java.io.Serializable
     */
    private static final long serialVersionUID = 2263144814025689516L;

    /** Constructs a new {@code SerializationException} without specified detail message. */
    public SerializationException() {}

    /**
     * Constructs a new {@code SerializationException} with specified detail message.
     *
     * @param msg The error message.
     */
    public SerializationException(final String msg) {
        super(msg);
    }

    /**
     * Constructs a new {@code SerializationException} with specified nested {@code Throwable}.
     *
     * @param cause The {@code Exception} or {@code Error} that caused this exception to be thrown.
     */
    public SerializationException(final Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new {@code SerializationException} with specified detail message and nested
     * {@code Throwable}.
     *
     * @param msg The error message.
     * @param cause The {@code Exception} or {@code Error} that caused this exception to be thrown.
     */
    public SerializationException(final String msg, final Throwable cause) {
        super(msg, cause);
    }
}
