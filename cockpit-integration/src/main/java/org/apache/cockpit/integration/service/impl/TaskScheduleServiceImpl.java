package org.apache.cockpit.integration.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.dto.integration.TaskScheduleDTO;
import org.apache.cockpit.common.bean.po.integration.TaskSchedulePO;
import org.apache.cockpit.common.enums.integration.ScheduleStatusEnum;
import org.apache.cockpit.common.utils.ConvertUtil;
import org.apache.cockpit.integration.quartz.QuartzJob;
import org.apache.cockpit.integration.service.TaskScheduleService;
import org.apache.cockpit.integration.utils.Utils;
import org.apache.cockpit.persistence.integration.TaskScheduleMapper;
import org.apache.commons.lang.StringUtils;
import org.quartz.*;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Task Schedule Service Implementation
 */
@Slf4j
@Service
public class TaskScheduleServiceImpl extends ServiceImpl<TaskScheduleMapper, TaskSchedulePO> implements TaskScheduleService {

    private final Scheduler scheduler;

    public TaskScheduleServiceImpl(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public String createTaskSchedule(TaskScheduleDTO taskScheduleDTO) throws SchedulerException {
        log.info("Creating task schedule: {}", taskScheduleDTO);

        // Check if schedule configuration already exists for the task definition
        if (existsByTaskDefinitionId(taskScheduleDTO.getTaskDefinitionId())) {
            throw new RuntimeException("Schedule configuration already exists for this task definition");
        }

        // Convert DTO to PO
        TaskSchedulePO po = ConvertUtil.sourceToTarget(taskScheduleDTO, TaskSchedulePO.class);

        // Save to database
        boolean saveResult = save(po);
        if (!saveResult) {
            throw new RuntimeException("Failed to save task schedule configuration");
        }

        log.info("Task schedule created successfully, ID: {}", po.getId());
        return po.getId();
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public boolean updateTaskSchedule(TaskScheduleDTO taskScheduleDTO) throws SchedulerException {
        log.info("Updating task schedule: {}", taskScheduleDTO);

        TaskSchedulePO existingSchedule = getById(taskScheduleDTO.getId());
        if (existingSchedule == null) {
            throw new RuntimeException("Task schedule configuration does not exist");
        }

        // Convert DTO to PO
        TaskSchedulePO taskSchedulePO = new TaskSchedulePO();
        BeanUtils.copyProperties(taskScheduleDTO, taskSchedulePO);

        // Update database
        boolean updateResult = updateById(taskSchedulePO);
        if (!updateResult) {
            throw new RuntimeException("Failed to update task schedule configuration");
        }

        log.info("Task schedule updated successfully, ID: {}", taskScheduleDTO.getId());
        return true;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public boolean deleteByTaskDefinitionId(String taskDefinitionId) {
        log.info("Deleting schedule by task definition ID: {}", taskDefinitionId);

        TaskSchedulePO taskSchedule = getByTaskDefinitionId(taskDefinitionId);
        if (taskSchedule == null) {
            log.warn("Schedule configuration not found for task definition ID: {}", taskDefinitionId);
            return true;
        }

        // Stop schedule
        stopSchedule(taskSchedule.getId());

        // Delete from database
        LambdaQueryWrapper<TaskSchedulePO> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(TaskSchedulePO::getTaskDefinitionId, taskDefinitionId);
        boolean removeResult = remove(queryWrapper);

        log.info("Schedule deletion by task definition ID completed: {}, Result: {}", taskDefinitionId, removeResult);
        return removeResult;
    }

    @Override
    public TaskSchedulePO getByTaskDefinitionId(String taskDefinitionId) {
        LambdaQueryWrapper<TaskSchedulePO> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(TaskSchedulePO::getTaskDefinitionId, taskDefinitionId);
        return getOne(queryWrapper);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Boolean startSchedule(String taskScheduleId) {
        log.info("Starting task schedule: {}", taskScheduleId);

        TaskSchedulePO taskSchedule = getById(taskScheduleId);
        if (taskSchedule == null) {
            throw new RuntimeException("Task schedule configuration does not exist");
        }
        try {
            JobKey jobKey = JobKey.jobKey(taskScheduleId);

            if (scheduler.checkExists(jobKey)) {
                scheduler.deleteJob(jobKey);
                log.info("Deleted existing Job: {}", taskScheduleId);
            }

            JobDetail jobDetail = createJobDetail(taskSchedule);
            Trigger trigger = createTrigger(taskSchedule);

            scheduler.scheduleJob(jobDetail, trigger);
        } catch (SchedulerException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to start schedule");
        }

        Date nextExecutionTime = Utils.getNextExecutionTime(taskSchedule.getCronExpression());
        updateNextScheduleTime(taskScheduleId, nextExecutionTime);
        updateScheduleStatus(taskScheduleId, ScheduleStatusEnum.RUNNING);

        log.info("Task schedule started successfully: {}", taskScheduleId);
        return true;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Boolean stopSchedule(String taskScheduleId) {
        log.info("Stopping task schedule: {}", taskScheduleId);

        TaskSchedulePO taskSchedule = getById(taskScheduleId);
        if (taskSchedule == null) {
            throw new RuntimeException("Task schedule configuration does not exist");
        }

        JobKey jobKey = JobKey.jobKey(taskScheduleId);
        try {
            if (scheduler.checkExists(jobKey)) {
                scheduler.deleteJob(jobKey);
            }
        } catch (SchedulerException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to stop schedule");
        }

        updateScheduleStatus(taskScheduleId, ScheduleStatusEnum.STOPPED);

        log.info("Task schedule stopped successfully: {}", taskScheduleId);
        return Boolean.TRUE;
    }

    @Override
    public boolean triggerSchedule(String taskScheduleId) throws SchedulerException {
        log.info("Triggering task schedule immediately: {}", taskScheduleId);

        TaskSchedulePO taskSchedule = getById(taskScheduleId);
        if (taskSchedule == null) {
            throw new RuntimeException("Task schedule configuration does not exist");
        }

        JobKey jobKey = JobKey.jobKey(taskScheduleId);
        scheduler.triggerJob(jobKey);

        log.info("Task schedule triggered successfully: {}", taskScheduleId);
        return true;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public boolean updateScheduleTime(String taskScheduleId, String cronExpression) throws SchedulerException {
        log.info("Updating schedule time: {}, Cron: {}", taskScheduleId, cronExpression);

        TaskSchedulePO taskSchedule = getById(taskScheduleId);
        if (taskSchedule == null) {
            throw new RuntimeException("Task schedule configuration does not exist");
        }

        taskSchedule.setCronExpression(cronExpression);
        updateById(taskSchedule);

        if (ScheduleStatusEnum.RUNNING.equals(taskSchedule.getScheduleStatus())) {
            stopSchedule(taskScheduleId);
            startSchedule(taskScheduleId);
        }

        log.info("Schedule time updated successfully: {}", taskScheduleId);
        return true;
    }

    @Override
    public List<TaskSchedulePO> getRunningSchedules() {
        LambdaQueryWrapper<TaskSchedulePO> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(TaskSchedulePO::getScheduleStatus, "RUNNING");
        return list(queryWrapper);
    }

    @Override
    public boolean existsByTaskDefinitionId(String taskDefinitionId) {
        LambdaQueryWrapper<TaskSchedulePO> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(TaskSchedulePO::getTaskDefinitionId, taskDefinitionId);
        return count(queryWrapper) > 0;
    }

    @Override
    public boolean updateScheduleStatus(String taskScheduleId, ScheduleStatusEnum status) {
        LambdaUpdateWrapper<TaskSchedulePO> updateWrapper = new LambdaUpdateWrapper<>();
        updateWrapper.set(TaskSchedulePO::getScheduleStatus, status)
                .eq(TaskSchedulePO::getId, taskScheduleId);
        return update(updateWrapper);
    }

    @Override
    public boolean updateLastScheduleTime(String taskScheduleId) {
        LambdaUpdateWrapper<TaskSchedulePO> updateWrapper = new LambdaUpdateWrapper<>();
        updateWrapper.set(TaskSchedulePO::getLastScheduleTime, new Date())
                .eq(TaskSchedulePO::getId, taskScheduleId);
        return update(updateWrapper);
    }

    @Override
    public boolean updateNextScheduleTime(String taskScheduleId, Date nextScheduleTime) {
        LambdaUpdateWrapper<TaskSchedulePO> updateWrapper = new LambdaUpdateWrapper<>();
        updateWrapper.set(TaskSchedulePO::getNextScheduleTime, nextScheduleTime)
                .eq(TaskSchedulePO::getId, taskScheduleId);
        return update(updateWrapper);
    }

    /**
     * Get the last 5 execution times based on Cron expression
     *
     * @param cronExpression Cron expression
     * @return List of the last 5 execution time strings
     */
    @Override
    public List<String> getLast5ExecutionTimesByCron(String cronExpression) {
        if (StringUtils.isBlank(cronExpression)) {
            throw new RuntimeException("Cron expression cannot be empty");
        }

        String cleanedCron = cronExpression.trim().replaceAll("\\s+", " ");

        if (!isValidCronExpression(cleanedCron)) {
            throw new RuntimeException("Invalid cron expression format: " + cleanedCron);
        }

        SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        CronExpression expression = null;
        try {
            expression = new CronExpression(cleanedCron);
        } catch (ParseException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to parse cron expression: " + cleanedCron + ", Error: " + e.getMessage());
        }

        Date now = new Date();
        List<String> executionTimes = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            now = expression.getNextValidTimeAfter(now);
            if (now == null) {
                break;
            }
            executionTimes.add(DATE_FORMAT.format(now));
        }
        return executionTimes;
    }

    private boolean isValidCronExpression(String cronExpression) {
        String[] parts = cronExpression.split(" ");
        return parts.length == 5 || parts.length == 6;
    }

    private JobDetail createJobDetail(TaskSchedulePO taskSchedule) {
        return JobBuilder.newJob(QuartzJob.class)
                .withIdentity(taskSchedule.getId())
                .usingJobData("taskScheduleId", taskSchedule.getId())
                .usingJobData("taskDefinitionId", taskSchedule.getTaskDefinitionId())
                .build();
    }

    private Trigger createTrigger(TaskSchedulePO taskSchedule) {
        return TriggerBuilder.newTrigger()
                .withIdentity(taskSchedule.getId())
                .withSchedule(CronScheduleBuilder.cronSchedule(taskSchedule.getCronExpression()))
                .build();
    }
}