package org.apache.cockpit.connectors.clickhouse.sink.client.executor;

import lombok.NonNull;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.clickhouse.sink.inject.*;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JdbcRowConverter implements Serializable {
    private static final Pattern NULLABLE = Pattern.compile("Nullable\\((.*)\\)");
    private static final Pattern LOW_CARDINALITY = Pattern.compile("LowCardinality\\((.*)\\)");
    private static final ClickhouseFieldInjectFunction DEFAULT_INJECT_FUNCTION =
            new StringInjectFunction();

    private final String[] projectionFields;
    private final Map<String, ClickhouseFieldInjectFunction> fieldInjectFunctionMap;
    private final Map<String, Function<SeaTunnelRow, Object>> fieldGetterMap;

    public JdbcRowConverter(
            @NonNull SeaTunnelRowType rowType,
            @NonNull Map<String, String> clickhouseTableSchema,
            @NonNull String[] projectionFields) {
        this.projectionFields = projectionFields;
        this.fieldInjectFunctionMap =
                createFieldInjectFunctionMap(projectionFields, clickhouseTableSchema);
        this.fieldGetterMap = createFieldGetterMap(projectionFields, rowType);
    }

    public PreparedStatement toExternal(SeaTunnelRow row, PreparedStatement statement)
            throws SQLException {
        for (int i = 0; i < projectionFields.length; i++) {
            String fieldName = projectionFields[i];
            Object fieldValue = fieldGetterMap.get(fieldName).apply(row);
            if (fieldValue == null) {
                // field does not exist in row
                // todo: do we need to transform to default value of each type
                statement.setObject(i + 1, null);
                continue;
            }
            fieldInjectFunctionMap
                    .getOrDefault(fieldName, DEFAULT_INJECT_FUNCTION)
                    .injectFields(statement, i + 1, fieldValue);
        }
        return statement;
    }

    private Map<String, ClickhouseFieldInjectFunction> createFieldInjectFunctionMap(
            String[] fields, Map<String, String> clickhouseTableSchema) {
        Map<String, ClickhouseFieldInjectFunction> fieldInjectFunctionMap = new HashMap<>();
        for (String field : fields) {
            String fieldType = clickhouseTableSchema.get(field);
            ClickhouseFieldInjectFunction injectFunction =
                    Arrays.asList(
                                    new ArrayInjectFunction(),
                                    new MapInjectFunction(),
                                    new BigDecimalInjectFunction(),
                                    new DateInjectFunction(),
                                    new DateTimeInjectFunction(),
                                    new LongInjectFunction(),
                                    new DoubleInjectFunction(),
                                    new FloatInjectFunction(),
                                    new IntInjectFunction(),
                                    new StringInjectFunction())
                            .stream()
                            .filter(f -> f.isCurrentFieldType(unwrapCommonPrefix(fieldType)))
                            .findFirst()
                            .orElse(new StringInjectFunction());
            fieldInjectFunctionMap.put(field, injectFunction);
        }
        return fieldInjectFunctionMap;
    }

    private Map<String, Function<SeaTunnelRow, Object>> createFieldGetterMap(
            String[] fields, SeaTunnelRowType rowType) {
        Map<String, Function<SeaTunnelRow, Object>> fieldGetterMap = new HashMap<>();
        for (int i = 0; i < fields.length; i++) {
            String fieldName = fields[i];
            int fieldIndex = rowType.indexOf(fieldName);
            fieldGetterMap.put(fieldName, row -> row.getField(fieldIndex));
        }
        return fieldGetterMap;
    }

    private String unwrapCommonPrefix(String fieldType) {
        Matcher nullMatcher = NULLABLE.matcher(fieldType);
        Matcher lowMatcher = LOW_CARDINALITY.matcher(fieldType);
        if (nullMatcher.matches()) {
            return nullMatcher.group(1);
        } else if (lowMatcher.matches()) {
            return lowMatcher.group(1);
        } else {
            return fieldType;
        }
    }
}
