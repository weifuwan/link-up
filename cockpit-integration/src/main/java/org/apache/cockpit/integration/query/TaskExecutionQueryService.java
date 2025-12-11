package org.apache.cockpit.integration.query;


import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.po.integration.TaskExecutionPO;
import org.apache.cockpit.common.bean.vo.integration.TaskExecutionVO;
import org.apache.cockpit.common.enums.integration.TaskExecutionStatus;
import org.apache.cockpit.common.utils.ConvertUtil;
import org.apache.cockpit.integration.service.TaskDefinitionService;
import org.apache.cockpit.persistence.integration.TaskExecutionMapper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Slf4j
@Service
public class TaskExecutionQueryService extends ServiceImpl<TaskExecutionMapper, TaskExecutionPO> {

    @Autowired
    private TaskDefinitionService taskDefinitionService;

    public TaskExecutionStatus getStatus(String executionId) {
        TaskExecutionPO execution = getById(executionId);
        return execution != null ? execution.getStatus() : null;
    }

    public List<TaskExecutionPO> getExecutionsByDefinition(String definitionId) {
        LambdaQueryWrapper<TaskExecutionPO> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(TaskExecutionPO::getDefinitionId, definitionId)
                .orderByDesc(TaskExecutionPO::getStartTime);
        return list(queryWrapper);
    }

    public List<TaskExecutionVO> getExecutionInfoByDefinitionId(String definitionId) {
        if (StringUtils.isBlank(definitionId)) {
            throw new RuntimeException("任务定义为空");
        }

        LambdaQueryWrapper<TaskExecutionPO> wrapper = new LambdaQueryWrapper<>();
        wrapper.eq(TaskExecutionPO::getDefinitionId, definitionId).orderByDesc(TaskExecutionPO::getCreateTime);
        List<TaskExecutionPO> taskExecutionPOS = getBaseMapper().selectList(wrapper);
        return ConvertUtil.sourceListToTarget(taskExecutionPOS, TaskExecutionVO.class);
    }

    public TaskExecutionVO selectById(String id) {
        TaskExecutionPO po = getById(id);
        if (po == null) {
            throw new RuntimeException("任务不存在");
        }
        TaskExecutionVO taskExecutionVO = ConvertUtil.sourceToTarget(po, TaskExecutionVO.class);

        // 设置源和目标类型
        taskExecutionVO.setSourceType(taskDefinitionService.selectById(po.getDefinitionId()).getSourceType());
        taskExecutionVO.setSinkType(taskDefinitionService.selectById(po.getDefinitionId()).getSinkType());

        return taskExecutionVO;
    }

    public String getTaskLog(String id) {
        TaskExecutionVO taskExecutionVO = selectById(id);
        if (taskExecutionVO == null) {
            throw new RuntimeException("执行任务不存在");
        }

        String content = "日志文件已丢失或不存在";
        File logFile = new File(taskExecutionVO.getLogPath());
        if (logFile.isFile()) {
            try {
                content = FileUtils.readFileToString(logFile, StandardCharsets.UTF_8);
            } catch (IOException e) {
                log.error("", e);
                throw new RuntimeException("获取日志失败");
            }
        }
        return content;
    }
}
