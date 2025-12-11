package org.apache.cockpit.connectors.api.event;

import java.io.Serializable;

public interface Event extends Serializable {

    long getCreatedTime();

    void setJobId(String jobId);

    String getJobId();

    EventType getEventType();
}
