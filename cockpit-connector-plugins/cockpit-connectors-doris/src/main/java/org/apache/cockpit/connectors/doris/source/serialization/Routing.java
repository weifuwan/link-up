package org.apache.cockpit.connectors.doris.source.serialization;


import org.apache.cockpit.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.cockpit.connectors.doris.exception.DorisConnectorException;
import org.apache.cockpit.connectors.doris.util.ErrorMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** present a Doris BE address. */
public class Routing {
    private static Logger logger = LoggerFactory.getLogger(Routing.class);

    private String host;
    private int port;

    public Routing(String routing) throws IllegalArgumentException {
        parseRouting(routing);
    }

    private void parseRouting(String routing) throws IllegalArgumentException {
        logger.debug("Parse Doris BE address: '{}'.", routing);
        String[] hostPort = routing.split(":");
        if (hostPort.length != 2) {
            logger.error("Format of Doris BE address '{}' is illegal.", routing);
            String errMsg =
                    String.format(ErrorMessages.ILLEGAL_ARGUMENT_MESSAGE, "routing", routing);
            throw new DorisConnectorException(DorisConnectorErrorCode.ROUTING_FAILED, errMsg);
        }
        this.host = hostPort[0];
        try {
            this.port = Integer.parseInt(hostPort[1]);
        } catch (NumberFormatException e) {
            logger.error(
                    String.format(
                            ErrorMessages.PARSE_NUMBER_FAILED_MESSAGE,
                            "Doris BE's port",
                            hostPort[1]));
            String errMsg =
                    String.format(ErrorMessages.PARSE_NUMBER_FAILED_MESSAGE, "routing", routing);
            throw new DorisConnectorException(DorisConnectorErrorCode.ROUTING_FAILED, errMsg, e);
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "Doris BE{" + "host='" + host + '\'' + ", port=" + port + '}';
    }
}
