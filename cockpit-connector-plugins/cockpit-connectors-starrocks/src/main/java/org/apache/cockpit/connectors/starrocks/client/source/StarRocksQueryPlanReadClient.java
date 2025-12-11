package org.apache.cockpit.connectors.starrocks.client.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.api.util.JsonUtils;
import org.apache.cockpit.connectors.api.util.RetryUtils;
import org.apache.cockpit.connectors.starrocks.client.HttpHelper;
import org.apache.cockpit.connectors.starrocks.client.source.model.QueryPartition;
import org.apache.cockpit.connectors.starrocks.client.source.model.QueryPlan;
import org.apache.cockpit.connectors.starrocks.config.SourceConfig;
import org.apache.cockpit.connectors.starrocks.config.StarRocksSourceTableConfig;
import org.apache.cockpit.connectors.starrocks.exception.StarRocksConnectorErrorCode;
import org.apache.cockpit.connectors.starrocks.exception.StarRocksConnectorException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class StarRocksQueryPlanReadClient {
    private RetryUtils.RetryMaterial retryMaterial;
    private SourceConfig sourceConfig;
    private final HttpHelper httpHelper = new HttpHelper();
    private final Map<String, StarRocksSourceTableConfig> tables;

    private static final long DEFAULT_SLEEP_TIME_MS = 1000L;

    public StarRocksQueryPlanReadClient(SourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
        this.retryMaterial =
                new RetryUtils.RetryMaterial(
                        sourceConfig.getMaxRetries(),
                        true,
                        exception -> true,
                        DEFAULT_SLEEP_TIME_MS);

        this.tables =
                sourceConfig.getTableConfigList().stream()
                        .collect(
                                Collectors.toMap(
                                        StarRocksSourceTableConfig::getTable, Function.identity()));
    }

    public List<QueryPartition> findPartitions(String table) {
        QueryPlan queryPlan = getQueryPlan(genQuerySql(table), table);
        Map<String, List<Long>> be2Tablets = selectBeForTablet(queryPlan);
        return tabletsMapToPartition(
                be2Tablets, queryPlan.getQueryPlan(), sourceConfig.getDatabase(), table);
    }

    private List<QueryPartition> tabletsMapToPartition(
            Map<String, List<Long>> be2Tablets,
            String opaquedQueryPlan,
            String database,
            String table)
            throws IllegalArgumentException {
        int tabletsSize = sourceConfig.getRequestTabletSize();
        List<QueryPartition> partitions = new ArrayList<>();
        for (Map.Entry<String, List<Long>> beInfo : be2Tablets.entrySet()) {
            log.debug("Generate partition with beInfo: '{}'.", beInfo);
            HashSet<Long> tabletSet = new HashSet<>(beInfo.getValue());
            beInfo.getValue().clear();
            beInfo.getValue().addAll(tabletSet);
            int first = 0;
            while (first < beInfo.getValue().size()) {
                Set<Long> partitionTablets =
                        new HashSet<>(
                                beInfo.getValue()
                                        .subList(
                                                first,
                                                Math.min(
                                                        beInfo.getValue().size(),
                                                        first + tabletsSize)));
                first = first + tabletsSize;
                QueryPartition partitionDefinition =
                        new QueryPartition(
                                database,
                                table,
                                beInfo.getKey(),
                                partitionTablets,
                                opaquedQueryPlan);
                log.debug("Generate one PartitionDefinition '{}'.", partitionDefinition);
                partitions.add(partitionDefinition);
            }
        }
        return partitions;
    }

    private Map<String, List<Long>> selectBeForTablet(QueryPlan queryPlan) {
        Map<String, List<Long>> beXTablets = new HashMap<>();
        queryPlan
                .getPartitions()
                .forEach(
                        (tabletId, routingList) -> {
                            int tabletCount = Integer.MAX_VALUE;
                            String candidateBe = "";
                            for (String beNode : routingList.getRoutings()) {
                                if (!beXTablets.containsKey(beNode)) {
                                    beXTablets.put(beNode, new ArrayList<>());
                                    candidateBe = beNode;
                                    break;
                                }
                                if (beXTablets.get(beNode).size() < tabletCount) {
                                    candidateBe = beNode;
                                    tabletCount = beXTablets.get(beNode).size();
                                }
                            }
                            beXTablets.get(candidateBe).add(Long.valueOf(tabletId));
                        });
        return beXTablets;
    }

    private QueryPlan getQueryPlan(String querySQL, String table) {

        List<String> nodeUrls = sourceConfig.getNodeUrls();
        // shuffle nodeUrls to ensure support for both random selection and high availability
        Collections.shuffle(nodeUrls);
        Map<String, Object> bodyMap = new HashMap<>();
        bodyMap.put("sql", querySQL);
        String body = JsonUtils.toJsonString(bodyMap);
        String respString = "";
        for (String feNode : nodeUrls) {
            String url =
                    new StringBuilder("http://")
                            .append(feNode)
                            .append("/api/")
                            .append(sourceConfig.getDatabase())
                            .append("/")
                            .append(table)
                            .append("/_query_plan")
                            .toString();
            try {
                respString =
                        RetryUtils.retryWithException(
                                () -> httpHelper.doHttpPost(url, getQueryPlanHttpHeader(), body),
                                retryMaterial);
                if (StringUtils.isNoneEmpty(respString)) {
                    return JsonUtils.parseObject(respString, QueryPlan.class);
                }
            } catch (Exception e) {
                log.error("Request query Plan From {} failed: {}", feNode, e.getMessage());
            }
        }

        throw new StarRocksConnectorException(
                StarRocksConnectorErrorCode.QUEST_QUERY_PLAN_FAILED,
                "query failed with empty response");
    }

    private String getBasicAuthHeader(String username, String password) {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
        return new StringBuilder("Basic ").append(new String(encodedAuth)).toString();
    }

    private Map<String, String> getQueryPlanHttpHeader() {
        Map<String, String> headerMap = new HashMap<>();
        headerMap.put("Content-Type", "application/json;charset=UTF-8");
        headerMap.put(
                "Authorization",
                getBasicAuthHeader(sourceConfig.getUsername(), sourceConfig.getPassword()));
        return headerMap;
    }

    private String genQuerySql(String table) {

        StarRocksSourceTableConfig starRocksSourceTableConfig = tables.get(table);
        SeaTunnelRowType seaTunnelRowType =
                starRocksSourceTableConfig.getCatalogTable().getSeaTunnelRowType();
        String columns =
                seaTunnelRowType.getFieldNames().length != 0
                        ? String.join(",", seaTunnelRowType.getFieldNames())
                        : "*";
        String scanFilter = starRocksSourceTableConfig.getScanFilter();
        String filter = scanFilter.isEmpty() ? "" : " where " + scanFilter;

        String sql =
                "select "
                        + columns
                        + " from "
                        + "`"
                        + sourceConfig.getDatabase()
                        + "`"
                        + "."
                        + "`"
                        + table
                        + "`"
                        + filter;
        log.debug("Generate query sql '{}'.", sql);
        return sql;
    }
}
