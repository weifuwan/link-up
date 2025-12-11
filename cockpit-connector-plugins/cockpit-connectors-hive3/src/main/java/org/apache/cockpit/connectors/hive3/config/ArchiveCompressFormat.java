package org.apache.cockpit.connectors.hive3.config;

/**
 * ZIP etc.:
 *
 * <p>Archive format: ZIP can compress multiple files and directories into a single archive.
 *
 * <p><br>
 * Gzip etc.:
 *
 * <p>Single file compression: Gzip compresses only one file at a time, without creating an archive.
 *
 * <p><br>
 */
public enum ArchiveCompressFormat {
    NONE(""),
    ZIP(".zip"),
    TAR(".tar"),
    TAR_GZ(".tar.gz"),
    GZ(".gz"),
    ;
    private final String archiveCompressCodec;

    ArchiveCompressFormat(String archiveCompressCodec) {
        this.archiveCompressCodec = archiveCompressCodec;
    }

    public String getArchiveCompressCodec() {
        return archiveCompressCodec;
    }
}
