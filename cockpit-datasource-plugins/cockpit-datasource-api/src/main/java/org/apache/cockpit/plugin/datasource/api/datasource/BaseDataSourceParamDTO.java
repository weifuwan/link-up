
package org.apache.cockpit.plugin.datasource.api.datasource;


import lombok.Data;
import org.apache.cockpit.common.constant.Constant;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.spi.modal.KeyValuePair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Basic datasource params submitted to api, each datasource plugin should have implementation.
 */
@Data
public abstract class BaseDataSourceParamDTO implements Serializable {

    protected String host;

    protected Integer port;

    protected String database;

    protected String username;

    protected String password;

    protected String driverLocation;

    // 使用 List 接收前端数组格式
    protected List<KeyValuePair> other;

    // 提供获取 Map 的方法
    public Map<String, String> getOtherAsMap() {
        if (other == null) {
            return new HashMap<>();
        }
        return other.stream()
                .filter(item -> item != null && item.getKey() != null && item.getValue() != null)
                .collect(Collectors.toMap(KeyValuePair::getKey, KeyValuePair::getValue));
    }

    // 设置 Map 的方法
    public void setOtherFromMap(Map<String, String> otherMap) {
        if (otherMap == null) {
            this.other = null;
            return;
        }
        this.other = otherMap.entrySet().stream()
                .map(entry -> new KeyValuePair(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }


    /**
     * extract the host and port from the address,
     * then set it
     *
     * @param address address like 'jdbc:mysql://host:port' or 'jdbc:hive2://zk1:port,zk2:port,zk3:port'
     */
    public void setHostAndPortByAddress(String address) {
        if (address == null) {
            throw new IllegalArgumentException("address is null.");
        }
        address = address.trim();

        int doubleSlashIndex = address.indexOf(Constant.DOUBLE_SLASH);
        // trim address like 'jdbc:mysql://host:port/xxx' ends with '/xxx'
        int slashIndex = address.indexOf(Constant.SLASH, doubleSlashIndex + 2);
        String hostPortString = slashIndex == -1 ? address.substring(doubleSlashIndex + 2)
                : address.substring(doubleSlashIndex + 2, slashIndex);

        ArrayList<String> hosts = new ArrayList<>();
        String portString = null;
        for (String hostPort : hostPortString.split(Constant.COMMA)) {
            String[] parts = hostPort.split(Constant.COLON);
            hosts.add(parts[0]);
            if (portString == null && parts.length > 1)
                portString = parts[1];
        }
        if (hosts.size() == 0 || portString == null) {
            throw new IllegalArgumentException(String.format("host:port '%s' illegal.", hostPortString));
        }

        this.host = String.join(Constant.COMMA, hosts);
        this.port = Integer.parseInt(portString);
    }

    /**
     * Get the datasource type
     * see{@link DbType}
     *
     * @return datasource type code
     */
    public abstract DbType getType();
}
