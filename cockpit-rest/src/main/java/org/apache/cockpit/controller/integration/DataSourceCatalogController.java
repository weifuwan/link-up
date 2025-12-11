package org.apache.cockpit.controller.integration;


import io.swagger.annotations.Api;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.entity.integration.QueryResult;
import org.apache.cockpit.common.bean.entity.result.Result;
import org.apache.cockpit.common.bean.vo.tree.ColumnOptionVO;
import org.apache.cockpit.common.bean.vo.tree.OptionVO;
import org.apache.cockpit.integration.service.DataSourceCatalogService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/v1/data-source/catalog")
@Api(tags = "数据集成-数据源Catalog")
public class DataSourceCatalogController {

    @Resource
    private DataSourceCatalogService dataSourceCatalogService;

    /**
     * 获取表列表
     *
     * @param id 数据源ID
     * @return 表列表
     */
    @GetMapping(value = "/{id}")
    public Result<List<OptionVO>> listTable(@PathVariable("id") String id) {
        return Result.buildSuc(dataSourceCatalogService.listTable(id));
    }

    /**
     * 获取字段列表
     *
     * @param id        数据源ID
     * @param tableName 表名
     * @return 字段列表
     */
    @GetMapping(value = "/column/{id}")
    public Result<List<ColumnOptionVO>> listColumn(@PathVariable("id") String id, @RequestParam("tableName") String tableName) {
        return Result.buildSuc(dataSourceCatalogService.listColumn(id, tableName));
    }

    @PostMapping(value = "/getTop20Data/{id}")
    public Result<QueryResult> getTop20Data(@PathVariable("id") String datasourceId, @RequestBody Map<String, Object> requestBody) {
        return Result.buildSuc(dataSourceCatalogService.getTop20Data(datasourceId, requestBody));
    }
}
