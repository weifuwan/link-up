package org.apache.cockpit.integration.validator;


import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.support.SFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.dto.integration.TaskDefinitionDTO;
import org.apache.cockpit.common.bean.po.integration.TaskDefinitionPO;
import org.apache.cockpit.integration.service.TaskDefinitionService;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;

/**
 * 任务定义参数校验器
 */
@Slf4j
@Component
public class TaskDefinitionValidator {

    @Resource
    private TaskDefinitionService taskDefinitionService;

    /**
     * 校验创建参数
     */
    public void validateCreateParams(TaskDefinitionDTO dto) {
        if (dto == null) {
            throw new RuntimeException("任务定义数据不能为空");
        }

        // 基础字段校验
        if (StringUtils.isEmpty(dto.getName())) {
            throw new RuntimeException("任务名称不能为空");
        }

        if (StringUtils.isEmpty(dto.getTaskParams())) {
            throw new RuntimeException("任务参数不能为空");
        }

        // 名称格式校验
        if (!dto.getName().matches("^[a-zA-Z0-9_\\-\\u4e00-\\u9fa5]{1,50}$")) {
            throw new RuntimeException("任务名称格式不正确，支持中文、英文、数字、下划线和连字符，最大50字符");
        }
    }

    /**
     * 验证字段唯一性
     */
    public void validateFieldUnique(String value, SFunction<TaskDefinitionPO, ?> fieldGetter) {
        if (org.apache.commons.lang.StringUtils.isNotBlank(value)) {
            LambdaQueryWrapper<TaskDefinitionPO> wrapper = new LambdaQueryWrapper<>();
            wrapper.eq(fieldGetter, value);
            if (taskDefinitionService.count(wrapper) > 0) {
                String errorMsg = "任务定义名称已存在：" + value;
                log.error(errorMsg);
                throw new RuntimeException(errorMsg);
            }
        }
    }
}