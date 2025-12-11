package org.apache.cockpit.integration.quartz;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.vo.integration.TaskDefinitionVO;
import org.apache.cockpit.common.enums.integration.ExecutionMode;
import org.apache.cockpit.integration.service.TaskDefinitionService;
import org.apache.cockpit.integration.service.TaskExecutionService;
import org.apache.cockpit.integration.service.TaskScheduleService;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Date;

/**
 * Quartz 任务执行类
 * 负责执行定时调度的任务
 */
@Slf4j
@Component
public class QuartzJob implements Job {

    @Resource
    private TaskExecutionService taskExecutionService;

    @Resource
    private TaskDefinitionService taskDefinitionService;

    @Resource
    private TaskScheduleService taskScheduleService;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        String taskDefinitionId = context.getMergedJobDataMap().getString("taskDefinitionId");
        String taskScheduleId = context.getMergedJobDataMap().getString("taskScheduleId");
        // 用户debug
        logExecutionContext(context);
        if (taskDefinitionId == null || taskScheduleId == null) {
            log.error("任务参数缺失: taskDefinitionId={}, taskScheduleId={}", taskDefinitionId, taskScheduleId);
            throw new JobExecutionException("任务参数缺失，无法执行任务");
        }

        TaskDefinitionVO taskDefinitionVO = taskDefinitionService.selectById(taskDefinitionId);
        if (taskDefinitionVO == null) {
            taskScheduleService.stopSchedule(taskScheduleId);
            log.error("{} 任务不存在，将删除对应的调度", taskDefinitionId);
        }

        log.info("开始执行定时任务: taskDefinitionId={}, taskScheduleId={}, fireTime={}",
                taskDefinitionId, taskScheduleId, context.getFireTime());

        try {
            updateLastScheduleTime(taskScheduleId);
            updateNextScheduleTime(context, taskScheduleId);
            taskExecutionService.execute(taskDefinitionId, ExecutionMode.SCHEDULED);

        } catch (Exception e) {
            log.error("定时任务执行异常: taskDefinitionId={}, taskScheduleId={}",
                    taskDefinitionId, taskScheduleId, e);
            if (shouldRefire(e)) {
                log.info("任务将重新执行: taskDefinitionId={}", taskDefinitionId);
                JobExecutionException jobException = new JobExecutionException(e);
                jobException.setRefireImmediately(true);
                throw jobException;
            } else {
                throw new JobExecutionException("任务执行异常", e, false);
            }
        }
    }

    /**
     * 更新最后执行时间
     */
    private void updateLastScheduleTime(String taskScheduleId) {
        try {
            taskScheduleService.updateLastScheduleTime(taskScheduleId);
            log.debug("更新最后执行时间成功: taskScheduleId={}", taskScheduleId);
        } catch (Exception e) {
            log.warn("更新最后执行时间失败: taskScheduleId={}", taskScheduleId, e);
        }
    }

    /**
     * 更新下次执行时间
     */
    private void updateNextScheduleTime(JobExecutionContext context, String taskScheduleId) {
        try {
            Date nextFireTime = context.getNextFireTime();
            if (nextFireTime != null) {
                taskScheduleService.updateNextScheduleTime(taskScheduleId, nextFireTime);
                log.debug("更新下次执行时间成功: taskScheduleId={}, nextFireTime={}",
                        taskScheduleId, nextFireTime);
            }
        } catch (Exception e) {
            log.warn("更新下次执行时间失败: taskScheduleId={}", taskScheduleId, e);
            // 这里不抛出异常，因为主要业务是任务执行，时间更新是辅助功能
        }
    }


    /**
     * 判断是否应该重新执行任务
     */
    private boolean shouldRefire(Exception e) {
        if (e instanceof java.net.ConnectException ||
                e instanceof java.sql.SQLTransientConnectionException ||
                e instanceof org.springframework.dao.TransientDataAccessResourceException) {
            return true;
        }

        // 默认不重试
        return false;
    }

    /**
     * 获取任务执行上下文信息（用于日志和监控）
     */
    private void logExecutionContext(JobExecutionContext context) {
        if (log.isDebugEnabled()) {
            log.debug("任务执行上下文: fireTime={}, nextFireTime={}, scheduledFireTime={}, jobRunTime={}ms",
                    context.getFireTime(),
                    context.getNextFireTime(),
                    context.getScheduledFireTime(),
                    context.getJobRunTime());
        }
    }
}