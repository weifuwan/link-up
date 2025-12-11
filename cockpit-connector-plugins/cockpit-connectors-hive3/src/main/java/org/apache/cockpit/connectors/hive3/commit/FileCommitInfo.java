package org.apache.cockpit.connectors.hive3.commit;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;

@Data
@AllArgsConstructor
public class FileCommitInfo implements Serializable {
    private static final long serialVersionUID = 7327659196051587339L;
    /**
     * Storage the commit info in map.
     *
     * <p>K is the file path need to be moved to target dir.
     *
     * <p>V is the target file path of the data file.
     */
    private final LinkedHashMap<String, String> needMoveFiles;

    /**
     * Storage the partition information in map.
     *
     * <p>K is the partition column's name.
     *
     * <p>V is the list of partition column's values.
     */
    private final LinkedHashMap<String, List<String>> partitionDirAndValuesMap;

    /** Storage the transaction directory */
    private final String transactionDir;
}
