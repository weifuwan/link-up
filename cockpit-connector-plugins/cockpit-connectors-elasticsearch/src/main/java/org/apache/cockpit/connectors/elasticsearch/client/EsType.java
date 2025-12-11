package org.apache.cockpit.connectors.elasticsearch.client;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Map;

@Getter
@AllArgsConstructor
public class EsType {

    public static final String AGGREGATE_METRIC_DOUBLE = "aggregate_metric_double";
    public static final String ALIAS = "alias";
    public static final String BINARY = "binary";
    public static final String BYTE = "byte";
    public static final String BOOLEAN = "boolean";
    public static final String COMPLETION = "completion";
    public static final String DATE = "date";
    public static final String DATETIME = "datetime";
    public static final String DATE_NANOS = "date_nanos";
    public static final String DENSE_VECTOR = "dense_vector";
    public static final String DOUBLE = "double";
    public static final String FLATTENED = "flattened";
    public static final String FLOAT = "float";
    public static final String GEO_POINT = "geo_point";
    public static final String GEO_SHAPE = "geo_shape";
    public static final String POINT = "point";
    public static final String INTEGER_RANGE = "integer_range";
    public static final String FLOAT_RANGE = "float_range";
    public static final String LONG_RANGE = "long_range";
    public static final String DOUBLE_RANGE = "double_range";
    public static final String DATE_RANGE = "date_range";
    public static final String IP_RANGE = "ip_range";
    public static final String HALF_FLOAT = "half_float";
    public static final String SCALED_FLOAT = "scaled_float";
    public static final String HISTOGRAM = "histogram";
    public static final String INTEGER = "integer";
    public static final String IP = "ip";
    public static final String JOIN = "join";
    public static final String KEYWORD = "keyword";
    public static final String LONG = "long";
    public static final String NESTED = "nested";
    public static final String OBJECT = "object";
    public static final String PERCOLATOR = "percolator";
    public static final String RANK_FEATURE = "rank_feature";
    public static final String RANK_FEATURES = "rank_features";
    public static final String SEARCH_AS_YOU_TYPE = "search_as_you_type";
    public static final String SHORT = "short";
    public static final String SHAPE = "shape";
    public static final String STRING = "string";
    public static final String SPARSE_VECTOR = "sparse_vector";
    public static final String TEXT = "text";
    public static final String MATCH_ONLY_TEXT = "match_only_text";
    public static final String TOKEN_COUNT = "token_count";
    public static final String UNSIGNED_LONG = "unsigned_long";
    public static final String VERSION = "version";

    private final String type;
    private final Map<String, Object> options;
}
