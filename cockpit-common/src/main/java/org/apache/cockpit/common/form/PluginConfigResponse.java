package org.apache.cockpit.common.form;

import lombok.Data;
import org.apache.cockpit.common.spi.enums.DbType;

import java.util.List;

/**
 * 插件配置响应
 */
@Data
public class PluginConfigResponse {
    private DbType pluginType;
    private List<FormFieldConfig> formFields;
}
