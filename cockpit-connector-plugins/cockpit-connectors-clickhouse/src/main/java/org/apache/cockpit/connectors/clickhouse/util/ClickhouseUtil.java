package org.apache.cockpit.connectors.clickhouse.util;

import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRecord;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.clickhouse.config.ClickhouseBaseOptions;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class ClickhouseUtil {

    public static List<ClickHouseNode> createNodes(ReadonlyConfig config) {
        return createNodes(
                config.get(ClickhouseBaseOptions.HOST),
                config.get(ClickhouseBaseOptions.DATABASE),
                config.get(ClickhouseBaseOptions.SERVER_TIME_ZONE),
                config.get(ClickhouseBaseOptions.USERNAME),
                config.get(ClickhouseBaseOptions.PASSWORD),
                config.get(ClickhouseBaseOptions.CLICKHOUSE_CONFIG));
    }

    public static List<ClickHouseNode> createNodes(
            String nodeAddress,
            String database,
            String serverTimeZone,
            String username,
            String password,
            Map<String, String> options) {
        return Arrays.stream(nodeAddress.split(","))
                .map(
                        address -> {
                            String[] nodeAndPort = address.split(":", 2);
                            ClickHouseNode.Builder builder =
                                    ClickHouseNode.builder()
                                            .host(nodeAndPort[0])
                                            .port(
                                                    ClickHouseProtocol.HTTP,
                                                    Integer.parseInt(nodeAndPort[1]))
                                            .database(database)
                                            .timeZone(serverTimeZone);
                            if (MapUtils.isNotEmpty(options)) {
                                for (Map.Entry<String, String> entry : options.entrySet()) {
                                    builder = builder.addOption(entry.getKey(), entry.getValue());
                                }
                            }

                            if (StringUtils.isNotEmpty(username)
                                    && StringUtils.isNotEmpty(password)) {
                                builder =
                                        builder.credentials(
                                                ClickHouseCredentials.fromUserAndPassword(
                                                        username, password));
                            }

                            return builder.build();
                        })
                .collect(Collectors.toList());
    }

    public static SeaTunnelRow convertToSeaTunnelRow(
            ClickHouseRecord record, SeaTunnelRowType seaTunnelRowType, String tableId) {
        Object[] values = new Object[seaTunnelRowType.getFieldNames().length];
        for (int i = 0; i < record.size(); i++) {
            if (record.getValue(i) == null || record.getValue(i).isNullOrEmpty()) {
                values[i] = null;
            } else {
                values[i] =
                        TypeConvertUtil.valueUnwrap(
                                seaTunnelRowType.getFieldType(i), record.getValue(i));
            }
        }
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(values);
        seaTunnelRow.setTableId(tableId);
        return seaTunnelRow;
    }

    public static TablePath extractTablePathFromSql(String sql) {
        try {
            Statement statement = CCJSqlParserUtil.parse(sql);

            TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
            Set<String> tableNames = tablesNamesFinder.getTables(statement);
            if (tableNames.size() == 1) {
                String tableFullName = tableNames.iterator().next();
                return TablePath.of(tableFullName);
            }

            return TablePath.DEFAULT;
        } catch (JSQLParserException e) {
            log.warn("Failed to parse SQL statement: {}, exception: {}", sql, e);
            return TablePath.DEFAULT;
        }
    }
}
