package org.apache.cockpit.integration.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.apache.cockpit.common.bean.po.integration.DataSourcePluginConfigPO;
import org.apache.cockpit.common.form.*;
import org.apache.cockpit.integration.service.DatasourcePluginService;
import org.apache.cockpit.persistence.integration.DatasourcePluginConfigMapper;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

@Service
public class DatasourcePluginServiceImpl extends ServiceImpl<DatasourcePluginConfigMapper, DataSourcePluginConfigPO> implements DatasourcePluginService {

    @Resource
    private DatasourcePluginConfigMapper configMapper;

    @Override
    public PluginConfigResponse getPluginConfig(String pluginType) {

        LambdaQueryWrapper<DataSourcePluginConfigPO> wrapper = new LambdaQueryWrapper<>();
        if (StringUtils.isNoneBlank(pluginType)) {
            wrapper.eq(DataSourcePluginConfigPO::getPluginType, pluginType);
        }

        DataSourcePluginConfigPO config = configMapper.selectOne(wrapper);
        if (config == null) {
            throw new RuntimeException("插件不存在: " + pluginType);
        }

        // 解析JSON schema并转换为表单配置
        List<FormFieldConfig> formFields = parseConfigSchema(JSONObject.parseObject(config.getConfigSchema()));

        PluginConfigResponse response = new PluginConfigResponse();
        response.setPluginType(config.getPluginType());
        response.setFormFields(formFields);

        return response;
    }

    /**
     * 解析JSON schema
     */
    private List<FormFieldConfig> parseConfigSchema(JSONObject configSchema) {
        List<FormFieldConfig> formFields = new ArrayList<>();

        // 这里根据实际的JSON结构进行解析
        // 示例实现
        JSONArray fields = configSchema.getJSONArray("fields");
        for (int i = 0; i < fields.size(); i++) {
            JSONObject field = fields.getJSONObject(i);
            FormFieldConfig formField = new FormFieldConfig();

            formField.setKey(field.getString("key"));
            formField.setLabel(field.getString("label"));
            formField.setType(FieldType.valueOf(field.getString("type").toUpperCase()));
            formField.setPlaceholder(field.getString("placeholder"));
            formField.setDefaultValue(field.get("defaultValue"));

            // 处理SELECT类型的选项
            if (FieldType.SELECT.equals(formField.getType()) && field.containsKey("options")) {
                JSONArray options = field.getJSONArray("options");
                List<Option> optionList = new ArrayList<>();
                for (int j = 0; j < options.size(); j++) {
                    JSONObject opt = options.getJSONObject(j);
                    Option option = new Option();
                    option.setLabel(opt.getString("label"));
                    option.setValue(opt.get("value"));
                    optionList.add(option);
                }
                formField.setOptions(optionList);
            }

            // 处理验证规则
            if (field.containsKey("rules")) {
                JSONArray rules = field.getJSONArray("rules");
                List<Rule> ruleList = new ArrayList<>();
                for (int j = 0; j < rules.size(); j++) {
                    JSONObject ruleJson = rules.getJSONObject(j);
                    Rule rule = new Rule();
                    rule.setPattern(ruleJson.getString("pattern"));
                    rule.setMessage(ruleJson.getString("message"));
                    rule.setMin(ruleJson.getInteger("min"));
                    rule.setMax(ruleJson.getInteger("max"));
                    rule.setRequired(ruleJson.getBoolean("required"));
                    ruleList.add(rule);
                }
                formField.setRules(ruleList);
            }

            formFields.add(formField);
        }

        return formFields;
    }
}
