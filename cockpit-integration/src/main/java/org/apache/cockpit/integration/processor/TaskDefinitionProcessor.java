package org.apache.cockpit.integration.processor;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.po.integration.TaskDefinitionPO;
import org.apache.cockpit.common.bean.vo.integration.TaskDefinitionVO;
import org.apache.cockpit.common.utils.ConvertUtil;
import org.apache.cockpit.integration.service.TaskDefinitionService;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 任务定义处理器
 * 负责任务定义的业务处理逻辑
 */
@Slf4j
@Component
public class TaskDefinitionProcessor {

    @Resource
    private TaskDefinitionService taskDefinitionService;

    /**
     * 初始化任务定义属性
     */
    public void initTaskDefinitionProperties(TaskDefinitionPO po) {
        po.initInsert();
        po.setSubmit(false);
    }

    /**
     * 初始化更新属性
     */
    public void initUpdateProperties(TaskDefinitionPO po) {
        po.initUpdate();
    }

    /**
     * 处理任务定义列表
     */
    public List<TaskDefinitionVO> processTaskDefinitions(List<TaskDefinitionVO> taskDefinitions) {
        if (CollectionUtils.isEmpty(taskDefinitions)) {
            return taskDefinitions;
        }

        Date now = new Date();

        return taskDefinitions.stream()
                .peek(task -> {
                    // 计算执行时长
                    if (task.getStartTime() != null) {
                        Date endTime = task.getEndTime() != null ? task.getEndTime() : now;
                        long durationMs = endTime.getTime() - task.getStartTime().getTime();

                        // 设置格式化后的时长字符串
                        task.setDuration(formatDuration(durationMs));

                        // 计算QPS并保留两位小数
                        if (task.getSourceTotalRecord() != null && task.getSourceTotalRecord() > 0 && durationMs > 0) {
                            double qps = (double) task.getSourceTotalRecord() / (durationMs / 1000.0);
                            task.setQps(Math.round(qps * 100.0) / 100.0);
                        } else {
                            task.setQps(0.0);
                        }
                    } else {
                        task.setDuration("0秒");
                        task.setQps(0.0);
                    }

                    // 计算同步大小
                    if (task.getSinkTotalBytes() != null && task.getSinkTotalBytes() > 0) {
                        task.setSyncSize(formatSyncSize(task.getSinkTotalBytes()));
                    } else {
                        task.setSyncSize("0M");
                    }
                })
                .collect(Collectors.toList());
    }

    /**
     * 发布任务定义
     */
    public TaskDefinitionVO publishTaskDefinition(String id) {
        TaskDefinitionPO po = taskDefinitionService.getById(id);
        if (po == null) {
            throw new RuntimeException("任务定义不存在，ID: " + id);
        }

        // 先将所有同名任务设置为下线状态
        LambdaUpdateWrapper<TaskDefinitionPO> updateWrapper = new LambdaUpdateWrapper<>();
        updateWrapper.eq(TaskDefinitionPO::getName, po.getName());
        updateWrapper.ne(TaskDefinitionPO::getId, id);

        TaskDefinitionPO updatePo = new TaskDefinitionPO();
        updatePo.setSubmit(false);
        taskDefinitionService.update(updatePo, updateWrapper);

        // 设置当前任务为上线状态
        po.setSubmit(true);
        initUpdateProperties(po);
        taskDefinitionService.updateById(po);

        log.info("成功发布任务定义: {}", id);
        return ConvertUtil.sourceToTarget(po, TaskDefinitionVO.class);
    }

    /**
     * 下线任务定义
     */
    public TaskDefinitionVO offlineTaskDefinition(String id) {
        TaskDefinitionPO po = taskDefinitionService.getById(id);
        if (po == null) {
            throw new RuntimeException("任务定义不存在，ID: " + id);
        }

        po.setSubmit(false);
        initUpdateProperties(po);
        taskDefinitionService.updateById(po);

        log.info("成功下线任务定义: {}", id);
        return ConvertUtil.sourceToTarget(po, TaskDefinitionVO.class);
    }

    /**
     * 格式化同步大小
     */
    private String formatSyncSize(Long bytes) {
        if (bytes == null || bytes <= 0) {
            return "0M";
        }

        double mb = bytes / (1024.0 * 1024.0);
        if (mb < 1024) {
            return String.format("%.2fM", mb);
        } else {
            double gb = mb / 1024.0;
            return String.format("%.2fG", gb);
        }
    }

    /**
     * 格式化时长
     */
    private String formatDuration(long durationMs) {
        if (durationMs <= 0) {
            return "0秒";
        }

        long seconds = durationMs / 1000;
        if (seconds < 60) {
            return seconds + "秒";
        }

        long minutes = seconds / 60;
        long remainingSeconds = seconds % 60;
        if (minutes < 60) {
            if (remainingSeconds == 0) {
                return minutes + "分钟";
            } else {
                return minutes + "分钟" + remainingSeconds + "秒";
            }
        }

        long hours = minutes / 60;
        long remainingMinutes = minutes % 60;
        if (hours < 24) {
            StringBuilder sb = new StringBuilder();
            sb.append(hours).append("小时");
            if (remainingMinutes > 0) {
                sb.append(remainingMinutes).append("分钟");
            }
            if (remainingSeconds > 0 && remainingMinutes == 0) {
                sb.append(remainingSeconds).append("秒");
            }
            return sb.toString();
        }

        long days = hours / 24;
        long remainingHours = hours % 24;
        StringBuilder sb = new StringBuilder();
        sb.append(days).append("天");
        if (remainingHours > 0) {
            sb.append(remainingHours).append("小时");
        }
        if (remainingMinutes > 0 && remainingHours == 0) {
            sb.append(remainingMinutes).append("分钟");
        }
        return sb.toString();
    }
}
