package org.apache.cockpit.common.bean.po.integration;

import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.cockpit.common.bean.po.BasePO;
import org.apache.cockpit.common.enums.integration.TaskExecutionTypeEnum;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.utils.JSONUtils;

@Data
@EqualsAndHashCode(callSuper = true)
@TableName("t_cockpit_integration_task_definition")
public class TaskDefinitionPO extends BasePO {

    /**
     * 任务名称
     */
    private String name;

    /**
     * source端的数据库类型
     */
    private DbType sourceType;

    /**
     * sink端的数据库类型
     */
    private DbType sinkType;

    /**
     * source端的数据库ID
     */
    private String sourceId;

    /**
     * sink端的数据库ID
     */
    private String sinkId;

    /**
     * 用户定义参数
     */
    @JsonDeserialize(using = JSONUtils.JsonDataDeserializer.class)
    @JsonSerialize(using = JSONUtils.JsonDataSerializer.class)
    private String taskParams;

    /**
     * 失败重试次数
     */
    private int failRetryTimes;

    /**
     * 失败重试间隔
     */
    private int failRetryInterval;

    /**
     * 任务是否上线
     */
    private Boolean submit;

    /**
     * 任务执行类型
     */
    private TaskExecutionTypeEnum taskExecuteType;

    /**
     * 任务描述
     */
    private String remark;
}


