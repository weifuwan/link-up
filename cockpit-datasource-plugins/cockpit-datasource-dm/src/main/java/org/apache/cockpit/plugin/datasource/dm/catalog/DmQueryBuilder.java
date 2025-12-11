package org.apache.cockpit.plugin.datasource.dm.catalog;

import com.alibaba.fastjson.JSONObject;

import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.utils.JSONUtils;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.dm.param.DmConnectionParam;

@Slf4j
public class DmQueryBuilder {

    public JSONObject buildSourceJson(String connectionParam, String sourceParam) {
        JSONObject sourceParamObj = JSONObject.parseObject(sourceParam);
        DmConnectionParam connectionParams = JSONUtils.parseObject(connectionParam, DmConnectionParam.class);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("jdbcUrl", connectionParams.getJdbcUrl());
        jsonObject.put("driver", connectionParams.getDriverClassName());
        jsonObject.put("username", connectionParams.getUsername());
        jsonObject.put("password", PasswordUtils.decodePassword(connectionParams.getPassword()));
        String tablePath = sourceParamObj.getString("table_path");
        if (StringUtils.isNotBlank(tablePath)) {
            String[] split = tablePath.split("\\.");
            if (split.length == 1) {
                tablePath = connectionParams.getUsername().toUpperCase() + "." + split[0];
            }
        } else {
            throw new RuntimeException("table path is null");
        }

        jsonObject.put("table_path", tablePath);
        jsonObject.put("plugin-type", DbType.DAMENG.getCode());
        jsonObject.put("dialect", DbType.DAMENG.getCode());
        jsonObject.put("database", connectionParams.getDatabase());
        jsonObject.put("driver_location", connectionParams.getDriverLocation());
        log.info("build source json is " + jsonObject.toJSONString());
        return jsonObject;
    }

    public JSONObject buildSinkJson(String connectionParams, String globalParam) {
        JSONObject globalParamObj = JSONObject.parseObject(globalParam);
        JSONObject sourceObj = globalParamObj.getJSONObject("source");
        JSONObject sinkObj = globalParamObj.getJSONObject("sink");
        JSONObject jsonObject = new JSONObject();

        DmConnectionParam connectionParam = JSONUtils.parseObject(connectionParams, DmConnectionParam.class);
        String sinkTablePath;
        if (sinkObj.getBoolean("autoCreateTable")) {
            sinkTablePath = sourceObj.getString("table_path");
            String[] split = sinkTablePath.split("\\.");
            if (split.length == 1) {
                sinkTablePath = connectionParam.getUsername() + "." + sinkTablePath;
            }
            jsonObject.put("generate_sink_sql", "true");
        } else {
            sinkTablePath = sinkObj.getString("table_path");
            String[] split = sinkTablePath.split("\\.");
            if (split.length == 1) {
                sinkTablePath = connectionParam.getUsername() + "." + sinkTablePath;
            }
        }
        jsonObject.put("jdbcUrl", connectionParam.getJdbcUrl());
        jsonObject.put("driver", connectionParam.getDriverClassName());
        jsonObject.put("username", connectionParam.getUsername());
        jsonObject.put("password", PasswordUtils.decodePassword(connectionParam.getPassword()));
        jsonObject.put("table", sinkTablePath.toUpperCase());
        jsonObject.put("plugin-type", DbType.DAMENG.getCode());
        jsonObject.put("dialect", DbType.DAMENG.getCode());
        jsonObject.put("database", connectionParam.getDatabase());
        jsonObject.put("driver_location", connectionParam.getDriverLocation());
        jsonObject.put("enable_upsert", sinkObj.getBoolean("enable_upsert"));
        jsonObject.put("data_save_mode", sinkObj.getString("data_save_mode"));
        jsonObject.put("batch_size", sinkObj.getString("batch_size"));
        jsonObject.put("field_ide", sinkObj.getString("field_ide"));
        log.info("build sink json is " + jsonObject.toJSONString());
        return jsonObject;
    }
}
