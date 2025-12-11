package org.apache.cockpit.integration.builder;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.vo.integration.DataSourceVO;
import org.apache.cockpit.integration.service.DataSourceService;
import org.apache.cockpit.plugin.datasource.api.datasource.DataSourceProcessor;
import org.apache.cockpit.plugin.datasource.api.utils.DataSourceUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;

/**
 * Task Parameter Builder
 */
@Slf4j
@Component
public class TaskDefinitionParamBuilder {

    @Resource
    private DataSourceService dataSourceService;

    /**
     * Build task parameters
     */
    public String buildTaskParams(String taskParams) {
        try {
            JSONObject jsonObject = JSON.parseObject(taskParams);

            // Validate task parameter structure
            validateTaskParamsStructure(jsonObject);

            // Build source configuration
            JSONObject sourceConfig = buildSourceConfig(jsonObject);

            // Build sink configuration
            JSONObject sinkConfig = buildSinkConfig(jsonObject);

            // Assemble final configuration
            JSONObject finalJson = new JSONObject();
            finalJson.put("source", sourceConfig);
            finalJson.put("sink", sinkConfig);

            log.debug("Task parameters built successfully: {}", finalJson.toJSONString());
            return finalJson.toJSONString();

        } catch (JSONException e) {
            log.error("Task parameter JSON format error: {}", taskParams, e);
            throw new RuntimeException("Task parameter format is incorrect");
        }
    }

    /**
     * Validate task parameter structure
     */
    private void validateTaskParamsStructure(JSONObject jsonObject) {
        if (!jsonObject.containsKey("source")) {
            throw new RuntimeException("Task parameters missing source configuration");
        }

        if (!jsonObject.containsKey("sink")) {
            throw new RuntimeException("Task parameters missing sink configuration");
        }

        JSONObject source = jsonObject.getJSONObject("source");
        if (source == null || StringUtils.isEmpty(source.getString("sourceId"))) {
            throw new RuntimeException("sourceId cannot be empty in source configuration");
        }

        JSONObject sink = jsonObject.getJSONObject("sink");
        if (sink == null || StringUtils.isEmpty(sink.getString("sinkId"))) {
            throw new RuntimeException("sinkId cannot be empty in sink configuration");
        }
    }

    /**
     * Build source configuration
     */
    private JSONObject buildSourceConfig(JSONObject taskParams) {
        JSONObject source = taskParams.getJSONObject("source");
        String sourceId = source.getString("sourceId");

        DataSourceVO sourceVo = dataSourceService.selectById(sourceId);
        if (sourceVo == null) {
            throw new RuntimeException("Data source does not exist, ID: " + sourceId);
        }

        if (StringUtils.isEmpty(sourceVo.getDbType())) {
            throw new RuntimeException("Data source type cannot be empty, ID: " + sourceId);
        }

        // Get data source processor
        DataSourceProcessor processor = DataSourceUtils.getDatasourceProcessor(sourceVo.getDbType());
        if (processor == null) {
            throw new RuntimeException("Unsupported data source type: " + sourceVo.getDbType());
        }

        JSONObject sourceConfig = processor.buildSourceJson(
                sourceVo.getConnectionParams(),
                source.toString()
        );

        if (sourceConfig == null) {
            throw new RuntimeException("Configuration returned by data source processor is empty, type: " + sourceVo.getDbType());
        }

        sourceConfig.put("sourceId", sourceId);
        sourceConfig.put("sourceName", sourceVo.getDbName());
        return sourceConfig;
    }

    /**
     * Build sink configuration
     */
    private JSONObject buildSinkConfig(JSONObject taskParams) {
        JSONObject sink = taskParams.getJSONObject("sink");
        String sinkId = sink.getString("sinkId");

        DataSourceVO sinkVo = dataSourceService.selectById(sinkId);
        if (sinkVo == null) {
            throw new RuntimeException("Target data source does not exist, ID: " + sinkId);
        }

        if (StringUtils.isEmpty(sinkVo.getDbType())) {
            throw new RuntimeException("Target data source type cannot be empty, ID: " + sinkId);
        }

        // Get data source processor
        DataSourceProcessor processor = DataSourceUtils.getDatasourceProcessor(sinkVo.getDbType());
        if (processor == null) {
            throw new RuntimeException("Unsupported target data source type: " + sinkVo.getDbType());
        }

        JSONObject sinkConfig = processor.buildSinkJson(
                sinkVo.getConnectionParams(),
                taskParams.toString()
        );

        if (sinkConfig == null) {
            throw new RuntimeException("Configuration returned by target data source processor is empty, type: " + sinkVo.getDbType());
        }

        sinkConfig.put("sinkId", sinkId);
        sinkConfig.put("sinkName", sinkVo.getDbName());

        return sinkConfig;
    }
}