package org.apache.cockpit.controller.integration;

import io.swagger.annotations.Api;
import org.apache.cockpit.common.bean.dto.integration.TaskDefinitionDTO;
import org.apache.cockpit.common.bean.entity.result.PaginationResult;
import org.apache.cockpit.common.bean.entity.result.Result;
import org.apache.cockpit.common.bean.vo.integration.TaskDefinitionVO;
import org.apache.cockpit.integration.service.TaskDefinitionService;
import org.apache.cockpit.integration.service.TaskScheduleService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.List;

/**
 * 任务定义控制器
 */
@RestController
@RequestMapping("/api/v1/task-definition")
@Api(tags = "数据集成-同步任务定义管理")
public class TaskDefinitionController {

    @Resource
    private TaskDefinitionService taskDefinitionService;

    @Resource
    private TaskScheduleService taskScheduleService;

    /**
     * 创建任务定义
     */
    @PostMapping
    public Result<TaskDefinitionVO> create(@RequestBody TaskDefinitionDTO dto) {
        TaskDefinitionVO result = taskDefinitionService.create(dto);
        return Result.buildSuc(result);
    }

    /**
     * 创建任务定义
     */
    @PostMapping("batch")
    public Result<Boolean> batch(@RequestBody TaskDefinitionDTO dto) {
        taskDefinitionService.batch(dto);
        return Result.buildSuc(Boolean.TRUE);
    }

    /**
     * 更新任务定义（创建新版本）
     */
    @PutMapping("/{id}")
    public Result<TaskDefinitionVO> update(@PathVariable String id,
                                           @RequestBody TaskDefinitionDTO dto) {
        TaskDefinitionVO result = taskDefinitionService.update(id, dto);
        return Result.buildSuc(result);
    }

    /**
     * 删除任务定义
     */
    @DeleteMapping("/{id}")
    public Result<Boolean> delete(@PathVariable String id) {
        // 删除任务定义
        taskDefinitionService.delete(id);

        // 级联删除对应的调度任务信息
        taskScheduleService.deleteByTaskDefinitionId(id);

        return Result.buildSuc(Boolean.TRUE);
    }

    /**
     * 根据ID查询任务定义
     */
    @GetMapping("/{id}")
    public Result<TaskDefinitionVO> getById(@PathVariable String id) {
        TaskDefinitionVO result = taskDefinitionService.selectById(id);
        return Result.buildSuc(result);
    }

    /**
     * 分页查询任务定义
     */
    @PostMapping("/page")
    public PaginationResult<TaskDefinitionVO> paging(@RequestBody TaskDefinitionDTO dto) {
        return taskDefinitionService.paging(dto);
    }

    /**
     * 条件查询所有任务定义
     */
    @GetMapping("/list")
    public Result<List<TaskDefinitionVO>> listAll(TaskDefinitionDTO queryDTO) {
        List<TaskDefinitionVO> result = taskDefinitionService.listAll(queryDTO);
        return Result.buildSuc(result);
    }

    /**
     * 批量删除任务定义
     */
    @DeleteMapping("/batch")
    public Result<Boolean> batchDelete(@RequestBody List<String> ids) {
        boolean result = taskDefinitionService.removeByIds(ids);
        return Result.buildSuc(result);
    }
}