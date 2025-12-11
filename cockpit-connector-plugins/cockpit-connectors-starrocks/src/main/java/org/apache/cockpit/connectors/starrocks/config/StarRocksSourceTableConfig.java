
package org.apache.cockpit.connectors.starrocks.config;

import lombok.Getter;
import com.google.common.collect.Lists;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TableIdentifier;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.catalog.TableSchema;
import org.apache.cockpit.connectors.api.catalog.schema.ReadonlyConfigParser;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
public class StarRocksSourceTableConfig implements Serializable {

    private final String table;

    private final CatalogTable catalogTable;

    private final String scanFilter;

    private StarRocksSourceTableConfig(
            String tableName, CatalogTable catalogTable, String scanFilter) {
        this.table = tableName;
        this.catalogTable = catalogTable;
        this.scanFilter = scanFilter;
    }

    public static StarRocksSourceTableConfig parseStarRocksSourceConfig(ReadonlyConfig config) {

        String table = config.get(StarRocksSourceOptions.TABLE);
        TablePath tablePath = TablePath.of(table);
        TableSchema tableSchema = new ReadonlyConfigParser().parse(config);
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("", tablePath),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "");

        return new StarRocksSourceTableConfig(
                table, catalogTable, config.get(StarRocksSourceOptions.SCAN_FILTER));
    }

    public static List<StarRocksSourceTableConfig> of(ReadonlyConfig config) {

        if (config.getOptional(StarRocksSourceOptions.TABLE_LIST).isPresent()) {
            List<Map<String, Object>> maps = config.get(StarRocksSourceOptions.TABLE_LIST);
            return maps.stream()
                    .map(ReadonlyConfig::fromMap)
                    .map(StarRocksSourceTableConfig::parseStarRocksSourceConfig)
                    .collect(Collectors.toList());
        }
        return Lists.newArrayList(parseStarRocksSourceConfig(config));
    }
}
