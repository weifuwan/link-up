package org.apache.cockpit.connectors.api.catalog.exception;

public class SeaTunnelException extends RuntimeException {

    /**
     * Required for serialization support.
     *
     * @see java.io.Serializable
     */
    private static final long serialVersionUID = 2263144814025689516L;

    /** Constructs a new {@code SeaTunnelException} without specified detail message. */
    public SeaTunnelException() {}

    /**
     * Constructs a new {@code SeaTunnelException} with specified detail message.
     *
     * @param msg The error message.
     */
    public SeaTunnelException(final String msg) {
        super(msg);
    }

    /**
     * Constructs a new {@code SeaTunnelException} with specified nested {@code Throwable}.
     *
     * @param cause The {@code Exception} or {@code Error} that caused this exception to be thrown.
     */
    public SeaTunnelException(final Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new {@code SeaTunnelException} with specified detail message and nested {@code
     * Throwable}.
     *
     * @param msg The error message.
     * @param cause The {@code Exception} or {@code Error} that caused this exception to be thrown.
     */
    public SeaTunnelException(final String msg, final Throwable cause) {
        super(msg, cause);
    }
}
