package org.apache.cockpit.persistence.integration;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.apache.cockpit.common.bean.dto.integration.TaskDefinitionDTO;
import org.apache.cockpit.common.bean.po.integration.TaskDefinitionPO;
import org.apache.cockpit.common.bean.vo.integration.TaskDefinitionVO;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface TaskDefinitionMapper extends BaseMapper<TaskDefinitionPO> {

    IPage<TaskDefinitionVO> selectTaskDefinitionWithLatestStatus(Page<TaskDefinitionVO> page, TaskDefinitionDTO dto);
}
