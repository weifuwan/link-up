package org.apache.cockpit.integration.service;

import com.baomidou.mybatisplus.extension.service.IService;
import org.apache.cockpit.common.bean.dto.integration.TaskDefinitionDTO;
import org.apache.cockpit.common.bean.entity.result.PaginationResult;
import org.apache.cockpit.common.bean.po.integration.TaskDefinitionPO;
import org.apache.cockpit.common.bean.vo.integration.TaskDefinitionVO;

import java.util.List;

public interface TaskDefinitionService extends IService<TaskDefinitionPO> {


    TaskDefinitionVO create(TaskDefinitionDTO dto);

    void batch(TaskDefinitionDTO dto);

    TaskDefinitionVO update(String id, TaskDefinitionDTO dto);

    boolean delete(String id);

    TaskDefinitionVO selectById(String id);

    Boolean existDataSource(String id);

    Boolean existDataSink(String id);

    PaginationResult<TaskDefinitionVO> paging(TaskDefinitionDTO queryDTO);

    List<TaskDefinitionVO> listAll(TaskDefinitionDTO queryDTO);


}
