package org.apache.cockpit.common.spi.datasource;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import lombok.Data;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.spi.modal.KeyValuePair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
@JsonInclude(Include.NON_NULL)
public abstract class BaseConnectionParam implements ConnectionParam {

    protected String username;

    protected String password;

    protected String address;

    protected String database;

    protected String jdbcUrl;

    protected String driverLocation;

    protected String driverClassName;

    protected String validationQuery;

    protected String compatibleMode;

    protected DbType dbType;

    protected List<KeyValuePair> other;

    public Map<String, String> getOtherAsMap() {
        if (other == null) {
            return new HashMap<>();
        }
        return other.stream()
                .filter(item -> item != null && item.getKey() != null && item.getValue() != null)
                .collect(Collectors.toMap(KeyValuePair::getKey, KeyValuePair::getValue));
    }


}
