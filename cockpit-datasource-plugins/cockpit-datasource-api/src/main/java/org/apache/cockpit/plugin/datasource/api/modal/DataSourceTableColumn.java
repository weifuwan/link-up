package org.apache.cockpit.plugin.datasource.api.modal;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DataSourceTableColumn {
    private String columnName;
    private String columnType;
    private String sourceType;
}
