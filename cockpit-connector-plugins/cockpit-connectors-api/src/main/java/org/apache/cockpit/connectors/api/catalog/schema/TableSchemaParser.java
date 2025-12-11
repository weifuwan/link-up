package org.apache.cockpit.connectors.api.catalog.schema;


import org.apache.cockpit.connectors.api.catalog.Column;
import org.apache.cockpit.connectors.api.catalog.ConstraintKey;
import org.apache.cockpit.connectors.api.catalog.PrimaryKey;
import org.apache.cockpit.connectors.api.catalog.TableSchema;

import java.util.List;

public interface TableSchemaParser<T> {

    /**
     * Parse schema config to TableSchema
     *
     * @param schemaConfig schema config
     * @return TableSchema
     */
    TableSchema parse(T schemaConfig);

    @Deprecated
    interface FieldParser<T> {

        /**
         * Parse field config to List<Column>
         *
         * @param schemaConfig schema config
         * @return List<Column> column list
         */
        List<Column> parse(T schemaConfig);
    }

    interface ColumnParser<T> {

        /**
         * Parse column config to List<Column>
         *
         * @param schemaConfig schema config
         * @return List<Column> column list
         */
        List<Column> parse(T schemaConfig);
    }

    interface ConstraintKeyParser<T> {

        /**
         * Parse constraint key config to ConstraintKey
         *
         * @param schemaConfig schema config
         * @return List<ConstraintKey> constraint key list
         */
        List<ConstraintKey> parse(T schemaConfig);
    }

    interface PrimaryKeyParser<T> {

        /**
         * Parse primary key config to PrimaryKey
         *
         * @param schemaConfig schema config
         * @return PrimaryKey
         */
        PrimaryKey parse(T schemaConfig);
    }
}
