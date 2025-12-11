package org.apache.cockpit.connectors.hive3.config;


import org.apache.cockpit.connectors.api.config.Option;
import org.apache.cockpit.connectors.api.config.Options;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;

public class HdfsSourceConfigOptions extends FileBaseSourceOptions {
    public static final Option<String> DEFAULT_FS =
            Options.key(FS_DEFAULT_NAME_KEY)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("HDFS namenode host");
}
