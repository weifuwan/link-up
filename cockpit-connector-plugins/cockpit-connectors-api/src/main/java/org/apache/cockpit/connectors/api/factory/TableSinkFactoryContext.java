package org.apache.cockpit.connectors.api.factory;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.sink.TablePlaceholderProcessor;

import java.util.Collection;
import java.util.Collections;

@Getter
public class TableSinkFactoryContext extends TableFactoryContext {

    private final CatalogTable catalogTable;

    @VisibleForTesting
    public TableSinkFactoryContext(
            CatalogTable catalogTable, ReadonlyConfig options, ClassLoader classLoader) {
        super(options, classLoader);
        if (catalogTable != null) {
            checkCatalogTableIllegal(Collections.singletonList(catalogTable));
        }
        this.catalogTable = catalogTable;
    }

    public static TableSinkFactoryContext replacePlaceholderAndCreate(
            CatalogTable catalogTable,
            ReadonlyConfig options,
            ClassLoader classLoader,
            Collection<String> excludeTablePlaceholderReplaceKeys) {
        ReadonlyConfig rewriteConfig =
                TablePlaceholderProcessor.replaceTablePlaceholder(
                        options, catalogTable, excludeTablePlaceholderReplaceKeys);
        return new TableSinkFactoryContext(catalogTable, rewriteConfig, classLoader);
    }
}
