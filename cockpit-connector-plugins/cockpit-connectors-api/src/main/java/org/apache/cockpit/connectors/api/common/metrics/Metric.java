package org.apache.cockpit.connectors.api.common.metrics;

import java.io.Serializable;

public interface Metric extends Serializable {

    /** Returns the name of the associated metric. */
    String name();

    /**
     * Return the measurement unit for the associated metric. Meant to provide further information
     * on the type of value measured by the user-defined metric. Doesn't affect the functionality of
     * the metric, it still remains a simple numeric value, but is used to populate the {@link
     * MetricTags#UNIT} tag in the metric's description.
     */
    Unit unit();
}
