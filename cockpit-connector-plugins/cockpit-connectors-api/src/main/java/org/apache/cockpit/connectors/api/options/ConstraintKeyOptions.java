package org.apache.cockpit.connectors.api.options;



import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.cockpit.connectors.api.catalog.ConstraintKey;
import org.apache.cockpit.connectors.api.config.Option;
import org.apache.cockpit.connectors.api.config.Options;

import java.util.List;
import java.util.Map;

public interface ConstraintKeyOptions {

    Option<List<Map<String, Object>>> CONSTRAINT_KEYS =
            Options.key("constraintKeys")
                    .type(new TypeReference<List<Map<String, Object>>>() {})
                    .noDefaultValue()
                    .withDescription(
                            "SeaTunnel Schema Constraint Keys. e.g. [{name: \"xx_index\", type: \"KEY\", columnKeys: [{columnName: \"name\", sortType: \"ASC\"}]}]");

    Option<String> CONSTRAINT_KEY_NAME =
            Options.key("constraintName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Constraint Key Name");

    Option<ConstraintKey.ConstraintType> CONSTRAINT_KEY_TYPE =
            Options.key("constraintType")
                    .enumType(ConstraintKey.ConstraintType.class)
                    .noDefaultValue()
                    .withDescription(
                            "SeaTunnel Schema Constraint Key Type, e.g. KEY, UNIQUE_KEY, FOREIGN_KEY");

    Option<List<Map<String, Object>>> CONSTRAINT_KEY_COLUMNS =
            Options.key("constraintColumns")
                    .type(new TypeReference<List<Map<String, Object>>>() {})
                    .noDefaultValue()
                    .withDescription(
                            "SeaTunnel Schema Constraint Key Columns. e.g. [{columnName: \"name\", sortType: \"ASC\"}]");

    Option<String> CONSTRAINT_KEY_COLUMN_NAME =
            Options.key("columnName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Constraint Key Column Name");

    Option<ConstraintKey.ColumnSortType> CONSTRAINT_KEY_COLUMN_SORT_TYPE =
            Options.key("sortType")
                    .enumType(ConstraintKey.ColumnSortType.class)
                    .defaultValue(ConstraintKey.ColumnSortType.ASC)
                    .withDescription(
                            "SeaTunnel Schema Constraint Key Column Sort Type, e.g. ASC, DESC");
}
