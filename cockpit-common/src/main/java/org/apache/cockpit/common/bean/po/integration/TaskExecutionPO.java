package org.apache.cockpit.common.bean.po.integration;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.cockpit.common.bean.po.BasePO;
import org.apache.cockpit.common.enums.integration.ExecutionMode;
import org.apache.cockpit.common.enums.integration.TaskExecutionStatus;
import org.apache.cockpit.common.enums.integration.TaskExecutionTypeEnum;

import java.util.Date;

@Data
@EqualsAndHashCode(callSuper = true)
@TableName("t_cockpit_integration_task_execution")
public class TaskExecutionPO extends BasePO {

    /**
     * 任务定义ID
     */
    private String definitionId;
    /**
     * 任务名称
     */
    private String taskName;

    /**
     * 任务执行状态
     */
    private TaskExecutionStatus status;

    /**
     * 引擎任务ID
     */
    private String engineTaskId;

    /**
     * 任务版本号
     */
    private Integer taskVersion;

    /**
     * 任务定义参数
     */
    private String taskParams;

    /**
     * 开始时间
     */
    private Date startTime;

    /**
     * 日志路径
     */
    private String logPath;

    /**
     * 结束时间
     */
    private Date endTime;

    /**
     * source端同步总数
     */
    private Long sourceTotalRecord;

    /**
     * source端同步总数
     */
    private Long sinkTotalRecord;

    /**
     * source端字节数
     */
    private Long sourceTotalBytes;

    /**
     * source端字节数
     */
    private Long sinkTotalBytes;

    /**
     * 执行模式
     */
    private ExecutionMode executionMode;

    /**
     * 任务执行类型
     */
    private TaskExecutionTypeEnum taskExecuteType;
}

