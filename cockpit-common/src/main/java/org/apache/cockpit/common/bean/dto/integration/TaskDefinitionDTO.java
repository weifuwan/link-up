package org.apache.cockpit.common.bean.dto.integration;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.cockpit.common.bean.dto.pagination.PaginationBaseDTO;
import org.apache.cockpit.common.enums.integration.TaskExecutionTypeEnum;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.utils.JSONUtils;

import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@ToString
public class TaskDefinitionDTO extends PaginationBaseDTO {

    /**
     * 任务名称
     */
    private String name;

    /**
     * 最新执行状态
     */
    private String status;

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
     * 任务执行类型
     */
    private TaskExecutionTypeEnum taskExecuteType;

    /**
     * 失败重试次数
     */
    private int failRetryTimes;

    /**
     * 失败重试间隔
     */
    private int failRetryInterval;

    /**
     * 开始时间
     */
    private String startTime;

    /**
     * 结束时间
     */
    private String endTime;

    /**
     * 任务是否上线
     */
    private Boolean flag;

    /**
     * 当前版本
     */
    private Integer currentVersion;
    /**
     * 任务描述
     */
    private String remark;

    /**
     * 调度配置
     */
    private TaskScheduleDTO taskScheduleDTO;

    /**
     * 多表配置
     */
    private List<String> multiTableList;

}


