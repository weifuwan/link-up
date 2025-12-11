package org.apache.cockpit.integration.service;

import com.baomidou.mybatisplus.extension.service.IService;
import org.apache.cockpit.common.bean.dto.integration.TaskScheduleDTO;
import org.apache.cockpit.common.bean.po.integration.TaskSchedulePO;
import org.apache.cockpit.common.enums.integration.ScheduleStatusEnum;
import org.quartz.SchedulerException;

import java.util.List;

/**
 * 任务调度服务接口
 * 提供任务调度相关的业务操作
 *
 */
public interface TaskScheduleService extends IService<TaskSchedulePO> {

    /**
     * 创建任务调度
     *
     * @param taskScheduleDTO 任务调度DTO
     * @return 创建的任务调度ID
     * @throws SchedulerException 调度器异常
     */
    String createTaskSchedule(TaskScheduleDTO taskScheduleDTO) throws SchedulerException;

    /**
     * 更新任务调度
     *
     * @param taskScheduleDTO 任务调度DTO
     * @return 是否更新成功
     * @throws SchedulerException 调度器异常
     */
    boolean updateTaskSchedule(TaskScheduleDTO taskScheduleDTO) throws SchedulerException;

    /**
     * 根据任务定义ID删除调度
     *
     * @param taskDefinitionId 任务定义ID
     * @return 是否删除成功
     * @throws SchedulerException 调度器异常
     */
    boolean deleteByTaskDefinitionId(String taskDefinitionId) ;

    /**
     * 根据任务定义ID查询调度信息
     *
     * @param taskDefinitionId 任务定义ID
     * @return 任务调度信息
     */
    TaskSchedulePO getByTaskDefinitionId(String taskDefinitionId);


    /**
     * 启动任务调度
     *
     * @param taskScheduleId 任务调度ID
     * @return 是否启动成功
     * @throws SchedulerException 调度器异常
     */
    Boolean startSchedule(String taskScheduleId);

    /**
     * 停止任务调度
     *
     * @param taskScheduleId 任务调度ID
     * @return 是否停止成功
     * @throws SchedulerException 调度器异常
     */
    Boolean stopSchedule(String taskScheduleId);

    /**
     * 立即执行一次任务
     *
     * @param taskScheduleId 任务调度ID
     * @return 是否触发成功
     * @throws SchedulerException 调度器异常
     */
    boolean triggerSchedule(String taskScheduleId) throws SchedulerException;

    /**
     * 更新调度时间
     *
     * @param taskScheduleId 任务调度ID
     * @param cronExpression Cron表达式
     * @return 是否更新成功
     * @throws SchedulerException 调度器异常
     */
    boolean updateScheduleTime(String taskScheduleId, String cronExpression) throws SchedulerException;

    /**
     * 获取所有运行中的调度任务
     *
     * @return 运行中的调度任务列表
     */
    List<TaskSchedulePO> getRunningSchedules();

    /**
     * 检查任务定义是否已有调度配置
     *
     * @param taskDefinitionId 任务定义ID
     * @return 是否已存在调度配置
     */
    boolean existsByTaskDefinitionId(String taskDefinitionId);

    /**
     * 更新调度状态
     *
     * @param taskScheduleId 任务调度ID
     * @param status         状态
     * @return 是否更新成功
     */
    boolean updateScheduleStatus(String taskScheduleId, ScheduleStatusEnum status);

    /**
     * 更新最后调度时间
     *
     * @param taskScheduleId 任务调度ID
     * @return 是否更新成功
     */
    boolean updateLastScheduleTime(String taskScheduleId);

    /**
     * 更新下次调度时间
     *
     * @param taskScheduleId   任务调度ID
     * @param nextScheduleTime 下次调度时间
     * @return 是否更新成功
     */
    boolean updateNextScheduleTime(String taskScheduleId, java.util.Date nextScheduleTime);

    List<String> getLast5ExecutionTimesByCron(String cronExpression);

}