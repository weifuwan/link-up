package org.apache.cockpit.connectors.hive3.config;


import org.apache.cockpit.connectors.hive3.sink.writer.OrcWriteStrategy;
import org.apache.cockpit.connectors.hive3.sink.writer.ParquetWriteStrategy;
import org.apache.cockpit.connectors.hive3.sink.writer.TextWriteStrategy;
import org.apache.cockpit.connectors.hive3.sink.writer.WriteStrategy;

import java.io.Serializable;
import java.util.Arrays;

public enum FileFormat implements Serializable {

    TEXT("txt") {
        @Override
        public WriteStrategy getWriteStrategy(FileSinkConfig fileSinkConfig) {
            return new TextWriteStrategy(fileSinkConfig);
        }

    },
    PARQUET("parquet") {
        @Override
        public WriteStrategy getWriteStrategy(FileSinkConfig fileSinkConfig) {
            return new ParquetWriteStrategy(fileSinkConfig);
        }
    },
    ORC("orc") {
        @Override
        public WriteStrategy getWriteStrategy(FileSinkConfig fileSinkConfig) {
            return new OrcWriteStrategy(fileSinkConfig);
        }

    },
    ;

    private final String[] suffix;

    FileFormat(String... suffix) {
        this.suffix = suffix;
    }

    public String getSuffix() {
        if (suffix.length > 0) {
            return "." + suffix[0];
        }
        return "";
    }

    public String[] getAllSuffix() {
        return Arrays.stream(suffix).map(suffix -> "." + suffix).toArray(String[]::new);
    }

//    public ReadStrategy getReadStrategy() {
//        return null;
//    }

    public WriteStrategy getWriteStrategy(FileSinkConfig fileSinkConfig) {
        return null;
    }
}
