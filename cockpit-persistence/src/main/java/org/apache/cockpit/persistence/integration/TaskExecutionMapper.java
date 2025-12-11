package org.apache.cockpit.persistence.integration;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.cockpit.common.bean.po.integration.TaskExecutionPO;
import org.apache.ibatis.annotations.Mapper;


@Mapper
public interface TaskExecutionMapper extends BaseMapper<TaskExecutionPO> {

}