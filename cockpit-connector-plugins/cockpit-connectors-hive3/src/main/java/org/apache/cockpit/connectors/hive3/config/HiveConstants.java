package org.apache.cockpit.connectors.hive3.config;

public class HiveConstants {

    public static final String CONNECTOR_NAME = "Hive";

    public static final String TEXT_INPUT_FORMAT_CLASSNAME =
            "org.apache.hadoop.mapred.TextInputFormat";
    public static final String TEXT_OUTPUT_FORMAT_CLASSNAME =
            "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
    public static final String PARQUET_INPUT_FORMAT_CLASSNAME =
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    public static final String PARQUET_OUTPUT_FORMAT_CLASSNAME =
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
    public static final String ORC_INPUT_FORMAT_CLASSNAME =
            "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
    public static final String ORC_OUTPUT_FORMAT_CLASSNAME =
            "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";
}
