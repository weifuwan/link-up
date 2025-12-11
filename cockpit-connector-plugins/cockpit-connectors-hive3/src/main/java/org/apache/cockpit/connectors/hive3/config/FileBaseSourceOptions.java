package org.apache.cockpit.connectors.hive3.config;


import org.apache.cockpit.connectors.api.config.Option;
import org.apache.cockpit.connectors.api.config.Options;
import org.apache.cockpit.connectors.api.serialization.text.TextFormatConstant;

import java.util.List;

public class FileBaseSourceOptions extends FileBaseOptions {
    public static final String DEFAULT_ROW_DELIMITER = "\n";

    public static final Option<FileFormat> FILE_FORMAT_TYPE =
            Options.key("file_format_type")
                    .objectType(FileFormat.class)
                    .noDefaultValue()
                    .withDescription(
                            "File format type, e.g. json, csv, text, parquet, orc, avro....");

    public static final Option<String> FIELD_DELIMITER =
            Options.key("field_delimiter")
                    .stringType()
                    .defaultValue(TextFormatConstant.SEPARATOR[0])
                    .withFallbackKeys("delimiter")
                    .withDescription(
                            "The separator between columns in a row of data. Only needed by `text` file format");

    public static final Option<String> ROW_DELIMITER =
            Options.key("row_delimiter")
                    .stringType()
                    .defaultValue(DEFAULT_ROW_DELIMITER)
                    .withDescription(
                            "The separator between rows in a file. Only needed by `text` file format");

    public static final Option<String> NULL_FORMAT =
            Options.key("null_format")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The string that represents a null value");

    public static final Option<Boolean> PARSE_PARTITION_FROM_PATH =
            Options.key("parse_partition_from_path")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether parse partition fields from file path");

    public static final Option<Long> SKIP_HEADER_ROW_NUMBER =
            Options.key("skip_header_row_number")
                    .longType()
                    .defaultValue(0L)
                    .withDescription("The number of rows to skip");

    public static final Option<List<String>> READ_PARTITIONS =
            Options.key("read_partitions")
                    .listType()
                    .noDefaultValue()
                    .withDescription("The partitions that the user want to read");

    public static final Option<List<String>> READ_COLUMNS =
            Options.key("read_columns")
                    .listType()
                    .noDefaultValue()
                    .withDescription("The columns list that the user want to read");


    public static final Option<String> XML_ROW_TAG =
            Options.key("xml_row_tag")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Specifies the tag name of the data rows within the XML file, only valid for XML files.");

    public static final Option<String> FILE_FILTER_PATTERN =
            Options.key("file_filter_pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "File pattern. The connector will filter some files base on the pattern.");

    public static final Option<String> FILE_FILTER_MODIFIED_START =
            Options.key("file_filter_modified_start")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "File modification time filter. The connector will filter some files base on the last modification start time (include start time). the default data format is yyyy-MM-dd HH:mm:ss");

    public static final Option<String> FILE_FILTER_MODIFIED_END =
            Options.key("file_filter_modified_end")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "File modification time filter. The connector will filter some files base on the last modification end time (not include end time). the default data format is yyyy-MM-dd HH:mm:ss");

    public static final Option<Integer> BINARY_CHUNK_SIZE =
            Options.key("binary_chunk_size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "The chunk size (in bytes) for reading binary files. Default is 1024 bytes. "
                                    + "Larger values may improve performance for large files but use more memory.Only valid when file_format_type is binary.");

    public static final Option<Boolean> BINARY_COMPLETE_FILE_MODE =
            Options.key("binary_complete_file_mode")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to read the complete file as a single chunk instead of splitting into chunks. "
                                    + "When enabled, the entire file content will be read into memory at once.Only valid when file_format_type is binary.");
}
