
package org.apache.cockpit.common.bean.po.integration;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.cockpit.common.bean.po.BasePO;
import org.apache.cockpit.common.enums.integration.ConnStatus;
import org.apache.cockpit.common.enums.integration.EnvironmentEnum;
import org.apache.cockpit.common.spi.enums.DbType;

@Data
@TableName("t_cockpit_datasource")
@EqualsAndHashCode(callSuper = true)
public class DataSourcePO extends BasePO {

    /**
     * 数据源名称
     */
    private String dbName;

    /**
     * 数据源类型
     */
    private DbType dbType;

    /**
     * 数据库连接参数
     */
    private String connectionParams;

    /**
     * 原始json
     */
    private String originalJson;

    /**
     * 描述
     */
    private String remark;

    /**
     * 连接状态
     */
    private ConnStatus connStatus;

    /**
     * 环境
     */
    private EnvironmentEnum environment;
}
