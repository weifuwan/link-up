package org.apache.cockpit.connectors.elasticsearch.dto.source;

public class IndexDocsCount {

    private String index;
    /** index docs count */
    private Long docsCount;

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public Long getDocsCount() {
        return docsCount;
    }

    public void setDocsCount(Long docsCount) {
        this.docsCount = docsCount;
    }
}
