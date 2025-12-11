package org.apache.cockpit.connectors.hive3.config;


import org.apache.cockpit.connectors.api.config.Option;
import org.apache.cockpit.connectors.api.config.Options;

import java.util.HashMap;
import java.util.Map;

public class HiveConfig {
    public static final Option<String> TABLE_NAME =
            Options.key("table_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Hive table name");
    public static final Option<String> METASTORE_URI =
            Options.key("metastore_uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Hive metastore uri");

    public static final Option<Boolean> ABORT_DROP_PARTITION_METADATA =
            Options.key("abort_drop_partition_metadata")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Flag to decide whether to drop partition metadata from Hive Metastore during an abort operation. Note: this only affects the metadata in the metastore, the data in the partition will always be deleted(data generated during the synchronization process).");

    public static final Option<String> HIVE_SITE_PATH =
            Options.key("hive_site_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The path of hive-site.xml");

    public static final Option<Map<String, String>> HADOOP_CONF =
            Options.key("hive.hadoop.conf")
                    .mapType()
                    .defaultValue(new HashMap<>())
                    .withDescription("Properties in hadoop conf");

    public static final Option<String> HADOOP_CONF_PATH =
            Options.key("hive.hadoop.conf-path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The specified loading path for the 'core-site.xml', 'hdfs-site.xml' files");
}
