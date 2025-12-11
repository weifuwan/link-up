package org.apache.cockpit.common.bean.vo.integration;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import org.apache.cockpit.common.enums.integration.ExecutionMode;
import org.apache.cockpit.common.enums.integration.TaskExecutionStatus;
import org.apache.cockpit.common.enums.integration.TaskExecutionTypeEnum;
import org.apache.cockpit.common.spi.enums.DbType;

import java.util.Date;

@Data
public class TaskDefinitionVO {

    /**
     * 主键
     */
    private String id;

    /**
     * 执行ID
     */
    private String executionId;

    /**
     * 任务名称
     */
    private String name;

    /**
     * 任务执行类型
     */
    private TaskExecutionTypeEnum taskExecuteType;

    /**
     * 任务执行类型名称
     */
    private String taskExecuteTypeName;

    public String getTaskExecuteTypeName() {
        return taskExecuteType.getDescription();
    }

    /**
     * 任务状态
     */
    private TaskExecutionStatus status;

    /**
     * source端的数据库类型
     */
    private DbType sourceType;

    /**
     * sink端的数据库类型
     */
    private DbType sinkType;

    /**
     * 用户定义参数
     */
    private String taskParams;

    /**
     * 失败重试次数
     */
    private int failRetryTimes;

    /**
     * 失败重试间隔
     */
    private int failRetryInterval;

    private ExecutionMode executionMode;
    private String executionModeName;

    /**
     * 任务是否上线
     */
    private boolean flag;

    /**
     * 当前版本
     */
    private Integer currentVersion;

    /**
     * 任务描述
     */
    private String remark;

    /**
     * 创建时间
     */
    @JsonFormat(pattern = "yyyy年MM月dd HH:mm:ss", timezone = "GMT+8")
    private Date createTime;

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

    /**
     * 执行开始时间
     */
    private Date startTime;

    /**
     * 执行结束时间
     */
    private Date endTime;

    /**
     * source同步数据量
     */
    private Long sourceTotalRecord;

    /**
     * sink同步数据量
     */
    private Long sinkTotalRecord;

    /**
     * source同步bytes
     */
    private Long sourceTotalBytes;

    /**
     * sink同步总bytes
     */
    private Long sinkTotalBytes;

    /**
     * 执行时长（秒）
     */
    private String duration;

    /**
     * 每秒处理记录数
     */
    private Double qps;

    /**
     * 同步大小
     */
    private String syncSize;

    private String cronExpression;

    private String scheduleStatus;

    private String taskScheduleId;

    @JsonFormat(pattern = "yyyy年MM月dd HH:mm:ss", timezone = "GMT+8")
    private Date lastScheduleTime;
    @JsonFormat(pattern = "yyyy年MM月dd HH:mm:ss", timezone = "GMT+8")
    private Date nextScheduleTime;
}


