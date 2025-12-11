package org.apache.cockpit.connectors.api.serialization.text;

import java.io.Serializable;
import java.util.regex.Pattern;

public class DefaultTextLineSplitor implements TextLineSplitor, Serializable {

    @Override
    public String[] spliteLine(String line, String seperator) {
        return line.split(Pattern.quote(seperator), -1);
    }
}
