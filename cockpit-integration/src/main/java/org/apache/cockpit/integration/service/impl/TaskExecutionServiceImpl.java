package org.apache.cockpit.integration.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.po.integration.TaskExecutionPO;
import org.apache.cockpit.common.bean.vo.integration.TaskExecutionVO;
import org.apache.cockpit.common.enums.integration.ExecutionMode;
import org.apache.cockpit.common.enums.integration.TaskExecutionStatus;
import org.apache.cockpit.connectors.api.common.metrics.TaskInfo;
import org.apache.cockpit.integration.manager.TaskExecutionManager;
import org.apache.cockpit.integration.metrics.TaskMetricsService;
import org.apache.cockpit.integration.query.TaskExecutionQueryService;
import org.apache.cockpit.integration.service.TaskExecutionService;
import org.apache.cockpit.persistence.integration.TaskExecutionMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class TaskExecutionServiceImpl extends ServiceImpl<TaskExecutionMapper, TaskExecutionPO>
        implements TaskExecutionService {

    @Resource
    private TaskExecutionManager taskExecutionManager;

    @Resource
    private TaskMetricsService taskMetricsService;

    @Resource
    private TaskExecutionQueryService taskExecutionQueryService;

    @PostConstruct
    public void init() {
        taskExecutionManager.init();
    }

    @PreDestroy
    public void destroy() {
        taskExecutionManager.destroy();
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void execute(String definitionId, ExecutionMode executionMode) {
        taskExecutionManager.execute(definitionId, executionMode);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void batchExecute(List<String> definitionIds, ExecutionMode executionMode) {
        taskExecutionManager.batchExecute(definitionIds, executionMode);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public boolean cancel(String executionId) {
        return taskExecutionManager.cancel(executionId);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void batchExecute(List<String> definitionIds) {
        taskExecutionManager.batchCancel(definitionIds);
    }

    @Override
    public TaskExecutionStatus getStatus(String executionId) {
        return taskExecutionQueryService.getStatus(executionId);
    }

    @Override
    public TaskInfo getEngineTaskInfo(String executionId) {
        return null;
    }

    @Override
    public List<TaskExecutionPO> getExecutionsByDefinition(String definitionId) {
        return taskExecutionQueryService.getExecutionsByDefinition(definitionId);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void cleanupCompletedExecutions(int days) {
        taskExecutionManager.cleanupCompletedExecutions(days);
    }

    @Override
    public Map<String, Object> getMetricsSummary(String timeRange, String taskType) {
        return taskMetricsService.getMetricsSummary(timeRange, taskType);
    }

    @Override
    public Map<String, Object> getSyncTrend(String timeRange) {
        return taskMetricsService.getSyncTrend(timeRange);
    }

    @Override
    public List<TaskExecutionVO> getExecutionInfoByDefinitionId(String definitionId) {
        return taskExecutionQueryService.getExecutionInfoByDefinitionId(definitionId);
    }

    @Override
    public TaskExecutionVO selectById(String id) {
        return taskExecutionQueryService.selectById(id);
    }

    @Override
    public String getTaskLog(String id) {
        return taskExecutionQueryService.getTaskLog(id);
    }
}