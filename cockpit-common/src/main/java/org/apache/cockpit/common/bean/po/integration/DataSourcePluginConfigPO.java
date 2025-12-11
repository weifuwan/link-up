
package org.apache.cockpit.common.bean.po.integration;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.cockpit.common.bean.po.BasePO;
import org.apache.cockpit.common.spi.enums.DbType;

@Data
@TableName("t_cockpit_datasource_plugin_config")
@EqualsAndHashCode(callSuper = true)
public class DataSourcePluginConfigPO extends BasePO {

    /**
     * 插件类型
     */
    private DbType pluginType;

    /**
     * 数据库连接参数
     */
    private String configSchema;

}
