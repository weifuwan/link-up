package org.apache.cockpit.connectors.api.template;

import org.apache.cockpit.connectors.api.catalog.exception.CommonError;
import org.apache.cockpit.connectors.api.sink.SaveModePlaceHolder;
import org.apache.commons.lang3.StringUtils;

public class SqlTemplate {
    public static void canHandledByTemplateWithPlaceholder(
            String createTemplate,
            String placeholder,
            String actualPlaceHolderValue,
            String tableName,
            String optionsKey) {
        if (createTemplate.contains(placeholder) && StringUtils.isBlank(actualPlaceHolderValue)) {
            throw CommonError.sqlTemplateHandledError(
                    tableName,
                    SaveModePlaceHolder.getDisplay(placeholder),
                    createTemplate,
                    placeholder,
                    optionsKey);
        }
    }
}
