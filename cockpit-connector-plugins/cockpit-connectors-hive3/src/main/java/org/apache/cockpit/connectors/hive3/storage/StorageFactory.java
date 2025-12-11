package org.apache.cockpit.connectors.hive3.storage;

public class StorageFactory {

    public static Storage getStorageType(String hiveSdLocation) {
        if (hiveSdLocation.startsWith(StorageType.FILE.name().toLowerCase())) {
            // Currently used in e2e, When Hive uses local files as storage, "file:" needs to be
            // replaced with "file:/" to avoid being recognized as HDFS storage.
            return new HDFSStorage(hiveSdLocation.replace("file:", "file:/"));
        } else {
            return new HDFSStorage(hiveSdLocation);
        }
    }
}
