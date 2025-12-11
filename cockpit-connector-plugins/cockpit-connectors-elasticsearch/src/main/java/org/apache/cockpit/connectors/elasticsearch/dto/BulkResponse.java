package org.apache.cockpit.connectors.elasticsearch.dto;

/** the response of bulk ES by http request */
public class BulkResponse {

    private boolean errors;
    private int took;
    private String response;

    public BulkResponse() {}

    public BulkResponse(boolean errors, int took, String response) {
        this.errors = errors;
        this.took = took;
        this.response = response;
    }

    public boolean isErrors() {
        return errors;
    }

    public void setErrors(boolean errors) {
        this.errors = errors;
    }

    public int getTook() {
        return took;
    }

    public void setTook(int took) {
        this.took = took;
    }

    public String getResponse() {
        return response;
    }

    public void setResponse(String response) {
        this.response = response;
    }
}
