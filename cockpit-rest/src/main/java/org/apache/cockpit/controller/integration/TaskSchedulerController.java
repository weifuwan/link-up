package org.apache.cockpit.controller.integration;

import io.swagger.annotations.Api;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.entity.result.Result;
import org.apache.cockpit.integration.service.TaskScheduleService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/v1/task-schedule")
@Api(tags = "任务调度管理")
public class TaskSchedulerController {

    @Resource
    private TaskScheduleService taskScheduleService;

    @GetMapping("/last5-execution-times")
    public Result<List<String>> getLast5ExecutionTimes(@RequestParam("cron") String cronExpression) {

        return Result.buildSuc(taskScheduleService.getLast5ExecutionTimesByCron(cronExpression));
    }

    @GetMapping("/stop-schedule")
    public Result<Boolean> stopSchedule(@RequestParam("taskScheduleId") String taskScheduleId) {
        return Result.buildSuc(taskScheduleService.stopSchedule(taskScheduleId));
    }
    @GetMapping("/start-schedule")
    public Result<Boolean> startSchedule(@RequestParam("taskScheduleId") String taskScheduleId) {
        return Result.buildSuc(taskScheduleService.startSchedule(taskScheduleId));
    }
}

