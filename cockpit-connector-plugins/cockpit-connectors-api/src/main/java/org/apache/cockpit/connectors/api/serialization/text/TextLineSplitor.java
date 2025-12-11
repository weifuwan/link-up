package org.apache.cockpit.connectors.api.serialization.text;

public interface TextLineSplitor {
    String[] spliteLine(String line, String splitor);
}
