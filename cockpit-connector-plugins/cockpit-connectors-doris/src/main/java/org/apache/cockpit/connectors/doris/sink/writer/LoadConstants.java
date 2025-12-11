package org.apache.cockpit.connectors.doris.sink.writer;

/** Constants for load. */
public class LoadConstants {
    public static final String COLUMNS_KEY = "columns";
    public static final String FIELD_DELIMITER_KEY = "column_separator";
    public static final String FIELD_DELIMITER_DEFAULT = "\t";
    public static final String LINE_DELIMITER_KEY = "line_delimiter";
    public static final String LINE_DELIMITER_DEFAULT = "\n";
    public static final String FORMAT_KEY = "format";
    public static final String JSON = "json";
    public static final String CSV = "csv";
    public static final String NULL_VALUE = "\\N";
    public static final String DORIS_DELETE_SIGN = "__DORIS_DELETE_SIGN__";
}
