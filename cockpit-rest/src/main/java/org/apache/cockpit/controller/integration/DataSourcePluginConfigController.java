package org.apache.cockpit.controller.integration;


import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.entity.result.Result;
import org.apache.cockpit.common.form.PluginConfigResponse;
import org.apache.cockpit.integration.service.DatasourcePluginService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

@Slf4j
@RestController
@RequestMapping("/api/v1/data-source/plugin/config")
@Api(tags = "数据集成-数据源配置插件")
public class DataSourcePluginConfigController {

    @Resource
    private DatasourcePluginService datasourcePluginService;

    /**
     * 数据源配置插件
     */
    @ApiOperation(value = "数据源配置插件")
    @GetMapping()
    public Result<PluginConfigResponse> paging(@RequestParam("pluginType") String pluginType) {
        return Result.buildSuc(datasourcePluginService.getPluginConfig(pluginType));
    }
}
