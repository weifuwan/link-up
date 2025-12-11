
package org.apache.cockpit.common.bean.dto.integration;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.cockpit.common.bean.dto.pagination.PaginationBaseDTO;
import org.apache.cockpit.common.enums.integration.ConnStatus;
import org.apache.cockpit.common.enums.integration.EnvironmentEnum;
import org.apache.cockpit.common.spi.enums.DbType;


@Data
@EqualsAndHashCode(callSuper = true)
public class DataSourceDTO extends PaginationBaseDTO {

    /**
     * 数据源名称
     */
    private String dbName;

    /**
     * 数据源类型
     */
    private DbType dbType;

    /**
     * 环境
     */
    private EnvironmentEnum environment;

    /**
     * 原始json
     */
    private String originalJson;

    /**
     * 连接参数
     */
    private String connectionParams;

    /**
     * 描述
     */
    private String remark;

    /**
     * 连接状态
     */
    private ConnStatus connStatus;
}
