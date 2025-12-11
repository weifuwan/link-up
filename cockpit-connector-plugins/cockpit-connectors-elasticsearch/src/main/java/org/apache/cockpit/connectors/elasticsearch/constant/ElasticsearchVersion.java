package org.apache.cockpit.connectors.elasticsearch.constant;


import org.apache.cockpit.connectors.elasticsearch.exception.ElasticsearchConnectorErrorCode;
import org.apache.cockpit.connectors.elasticsearch.exception.ElasticsearchConnectorException;

public enum ElasticsearchVersion {
    ES2(2),
    ES5(5),
    ES6(6),
    ES7(7),
    ES8(8);

    private int version;

    ElasticsearchVersion(int version) {
        this.version = version;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public static ElasticsearchVersion get(int version) {
        for (ElasticsearchVersion elasticsearchVersion : ElasticsearchVersion.values()) {
            if (elasticsearchVersion.getVersion() == version) {
                return elasticsearchVersion;
            }
        }
        throw new ElasticsearchConnectorException(
                ElasticsearchConnectorErrorCode.GET_ES_VERSION_FAILED,
                String.format("version=%d,fail fo find ElasticsearchVersion.", version));
    }

    public static ElasticsearchVersion get(String clusterVersion) {
        String[] versionArr = clusterVersion.split("\\.");
        int version = Integer.parseInt(versionArr[0]);
        return get(version);
    }
}
