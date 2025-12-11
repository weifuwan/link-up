package org.apache.cockpit.integration.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.dto.integration.TaskDefinitionDTO;
import org.apache.cockpit.common.bean.dto.integration.TaskScheduleDTO;
import org.apache.cockpit.common.bean.entity.result.PaginationResult;
import org.apache.cockpit.common.bean.po.integration.TaskDefinitionPO;
import org.apache.cockpit.common.bean.vo.integration.TaskDefinitionVO;
import org.apache.cockpit.common.enums.integration.ScheduleStatusEnum;
import org.apache.cockpit.common.utils.ConvertUtil;
import org.apache.cockpit.integration.builder.TaskDefinitionParamBuilder;
import org.apache.cockpit.integration.processor.TaskDefinitionProcessor;
import org.apache.cockpit.integration.service.TaskDefinitionService;
import org.apache.cockpit.integration.service.TaskScheduleService;
import org.apache.cockpit.integration.validator.TaskDefinitionValidator;
import org.apache.cockpit.persistence.integration.TaskDefinitionMapper;
import org.quartz.SchedulerException;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Task Definition Service Implementation
 * Responsible for CRUD operations, etc. for task definitions
 */
@Slf4j
@Service
public class TaskDefinitionServiceImpl extends ServiceImpl<TaskDefinitionMapper, TaskDefinitionPO>
        implements TaskDefinitionService {

    @Resource
    private TaskDefinitionValidator taskDefinitionValidator;

    @Resource
    private TaskDefinitionParamBuilder taskDefinitionParamBuilder;

    @Resource
    private TaskDefinitionProcessor taskDefinitionProcessor;

    @Resource
    private TaskScheduleService taskScheduleService;

    @Override
    public TaskDefinitionVO create(TaskDefinitionDTO dto) {
        log.info("Starting to create task definition: {}", dto.getName());

        // Parameter validation
        taskDefinitionValidator.validateCreateParams(dto);
        taskDefinitionValidator.validateFieldUnique(dto.getName(), TaskDefinitionPO::getName);

        // Build task parameters
        String taskParams = taskDefinitionParamBuilder.buildTaskParams(dto.getTaskParams());

        // Create task definition
        TaskDefinitionPO po = ConvertUtil.sourceToTarget(dto, TaskDefinitionPO.class);
        po.setTaskParams(taskParams);
        taskDefinitionProcessor.initTaskDefinitionProperties(po);

        boolean saveResult = save(po);
        if (!saveResult) {
            log.error("Failed to create task definition, data: {}", dto);
            throw new RuntimeException("Failed to create task definition");
        }

        try {
            if (dto.getTaskScheduleDTO() == null) {
                throw new RuntimeException("Schedule configuration cannot be empty");
            }
            TaskScheduleDTO taskScheduleDTO = dto.getTaskScheduleDTO();
            taskScheduleDTO.setTaskDefinitionId(po.getId());
            String taskSchedule = taskScheduleService.createTaskSchedule(taskScheduleDTO);

            if (taskScheduleDTO.getScheduleStatus().equals(ScheduleStatusEnum.RUNNING)) {
                taskScheduleService.startSchedule(taskSchedule);
            }
        } catch (SchedulerException e) {
            throw new RuntimeException("Failed to create task schedule");
        }

        log.info("Successfully created task definition, ID: {}, Name: {}", po.getId(), po.getName());
        return ConvertUtil.sourceToTarget(po, TaskDefinitionVO.class);
    }

    @Override
    public void batch(TaskDefinitionDTO dto) {
        log.info("Starting batch creation of task definitions, Source ID: {}, Target ID: {}, Table count: {}",
                dto.getSourceId(), dto.getSinkId(), dto.getMultiTableList().size());

        if (dto.getSourceId() == null || dto.getSinkId() == null) {
            throw new RuntimeException("Source ID and Target ID cannot be empty");
        }

        if (dto.getMultiTableList() == null || dto.getMultiTableList().isEmpty()) {
            log.warn("Table list for batch task definition creation is empty");
            return;
        }

        String baseTemplate = "{\"source\":{\"sourceId\":\"%s\",\"table_path\":\"%s\",\"filterConditions\":[]},\"sink\":{\"sinkId\":\"%s\",\"autoCreateTable\":true,\"data_save_mode\":\"APPEND_DATA\",\"batch_size\":2000,\"enable_upsert\":false}}";

        int successCount = 0;
        int failCount = 0;

        for (String tablePath : dto.getMultiTableList()) {
            try {
                log.info("Creating task definition for table {}", tablePath);
                String taskParams = String.format(baseTemplate, dto.getSourceId(), tablePath, dto.getSinkId());
                TaskDefinitionDTO taskDto = new TaskDefinitionDTO();
                taskDto.setName("multi_task_" + tablePath);

                taskDto.setTaskParams(taskParams);
                taskDto.setSourceId(dto.getSourceId());
                taskDto.setSinkId(dto.getSinkId());
                taskDto.setSourceType(dto.getSourceType());
                taskDto.setSinkType(dto.getSinkType());
                taskDto.setFlag(dto.getFlag());
                taskDto.setTaskExecuteType(dto.getTaskExecuteType());
                taskDto.setTaskScheduleDTO(dto.getTaskScheduleDTO());

                TaskDefinitionVO result = this.create(taskDto);
                successCount++;
                log.info("Successfully created task definition for table {}, Task ID: {}", tablePath, result.getId());

            } catch (Exception e) {
                failCount++;
                log.error("Failed to create task definition for table {}: {}", tablePath, e.getMessage(), e);
            }
        }

        log.info("Batch task definition creation completed, Success: {}, Failed: {}", successCount, failCount);

        if (failCount > 0) {
            throw new RuntimeException(String.format("Batch task definition creation completed, but %d failed", failCount));
        }
    }

    private String generateTaskName(String baseName, String tablePath) {
        String tableName = tablePath;
        if (tablePath.contains(".")) {
            tableName = tablePath.substring(tablePath.lastIndexOf(".") + 1);
        }

        tableName = tableName.replaceAll("[^a-zA-Z0-9_]", "_");

        return baseName + "_" + tableName;
    }

    private String generateDefaultTaskName(String tablePath) {
        String tableName = tablePath;
        if (tablePath.contains(".")) {
            tableName = tablePath.substring(tablePath.lastIndexOf(".") + 1);
        }

        tableName = tableName.replaceAll("[^a-zA-Z0-9_]", "_");

        return "task_" + tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public TaskDefinitionVO update(String id, TaskDefinitionDTO dto) {
        log.info("Starting to update task definition: {}", id);

        TaskDefinitionPO po = getById(id);
        if (po == null) {
            throw new RuntimeException("Task definition does not exist, ID: " + id);
        }

        // Update properties
        po.setSubmit(false);
        taskDefinitionProcessor.initUpdateProperties(po);

        boolean updateResult = updateById(po);
        if (!updateResult) {
            log.error("Failed to update task definition, ID: {}", id);
            throw new RuntimeException("Failed to update task definition");
        }

        TaskDefinitionVO vo = ConvertUtil.sourceToTarget(po, TaskDefinitionVO.class);
        vo.setCurrentVersion(1);

        log.info("Successfully updated task definition: {}", id);
        return vo;
    }

    @Override
    public boolean delete(String id) {
        log.info("Deleting task definition: {}", id);
        return removeById(id);
    }

    @Override
    public TaskDefinitionVO selectById(String id) {
        TaskDefinitionPO po = getById(id);
        return ConvertUtil.sourceToTarget(po, TaskDefinitionVO.class);
    }

    @Override
    public Boolean existDataSource(String id) {
        if (StringUtils.isEmpty(id)) {
            log.warn("ID is empty when checking data source existence");
            return false;
        }

        LambdaQueryWrapper<TaskDefinitionPO> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(TaskDefinitionPO::getSourceId, id);

        long count = count(queryWrapper);
        return count > 0;
    }

    @Override
    public Boolean existDataSink(String id) {
        if (StringUtils.isEmpty(id)) {
            log.warn("Data source ID is empty");
            return false;
        }

        LambdaQueryWrapper<TaskDefinitionPO> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(TaskDefinitionPO::getSinkId, id);

        long count = count(queryWrapper);

        return count > 0;
    }

    @Override
    public PaginationResult<TaskDefinitionVO> paging(TaskDefinitionDTO dto) {
        Page<TaskDefinitionVO> page = new Page<>(dto.getPageNo(), dto.getPageSize());
        IPage<TaskDefinitionVO> resultPage = getBaseMapper().selectTaskDefinitionWithLatestStatus(page, dto);

        // Process task definition data
        List<TaskDefinitionVO> processedRecords = taskDefinitionProcessor.processTaskDefinitions(resultPage.getRecords());

        return PaginationResult.buildSuc(processedRecords, resultPage);
    }

    @Override
    public List<TaskDefinitionVO> listAll(TaskDefinitionDTO queryDTO) {
        LambdaQueryWrapper<TaskDefinitionPO> queryWrapper = new LambdaQueryWrapper<>();

        if (StringUtils.hasText(queryDTO.getName())) {
            queryWrapper.like(TaskDefinitionPO::getName, queryDTO.getName());
        }
        if (queryDTO.getFlag() != null) {
            queryWrapper.eq(TaskDefinitionPO::getSubmit, queryDTO.getFlag());
        }

        queryWrapper.orderByDesc(TaskDefinitionPO::getCreateTime);

        List<TaskDefinitionPO> poList = list(queryWrapper);
        return poList.stream()
                .map(po -> ConvertUtil.sourceToTarget(po, TaskDefinitionVO.class))
                .collect(Collectors.toList());
    }
}