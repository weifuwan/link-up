package org.apache.cockpit.common.bean.vo.integration;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import org.apache.cockpit.common.enums.integration.ExecutionMode;
import org.apache.cockpit.common.enums.integration.TaskExecutionStatus;
import org.apache.cockpit.common.enums.integration.TaskExecutionTypeEnum;
import org.apache.cockpit.common.spi.enums.DbType;

import java.util.Date;

@Data
public class TaskExecutionVO {

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
    @JsonFormat(pattern = "yyyy年MM月dd HH:mm:ss", timezone = "GMT+8")
    private Date startTime;

    /**
     * 日志路径
     */
    private String logPath;

    /**
     * 结束时间
     */
    @JsonFormat(pattern = "yyyy年MM月dd HH:mm:ss", timezone = "GMT+8")
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

    private String taskExecuteTypeName;

    public String getTaskExecuteTypeName() {
        return taskExecuteType.getDescription();
    }

    private String id;

    /**
     * source端的数据库类型
     */
    private DbType sourceType;

    /**
     * sink端的数据库类型
     */
    private DbType sinkType;

    /**
     * 创建时间
     */
    @JsonFormat(pattern = "yyyy年MM月dd HH:mm:ss", timezone = "GMT+8")
    protected Date createTime;

    /**
     * 创建人
     */
    private String createBy;

    /**
     * 更新时间
     */
    @JsonFormat(pattern = "yyyy年MM月dd HH:mm:ss", timezone = "GMT+8")
    private Date updateTime;

    /**
     * 更新人
     */
    private String updateBy;

}

