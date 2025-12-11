package org.apache.cockpit.connectors.api.source;

import java.io.Serializable;

/** An interface for all the Split types to extend. */
public interface SourceSplit extends Serializable {

    /**
     * Get the split id of this source split.
     *
     * @return id of this source split.
     */
    String splitId();
}
