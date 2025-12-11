package org.apache.cockpit.plugin.datasource.cache.catalog;


import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.utils.JSONUtils;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.cache.param.CacheConnectionParam;

@Slf4j
public class CacheQueryBuilder {

    public JSONObject buildSourceJson(String connectionParam, String sourceParam) {
        JSONObject sourceParamObj = JSONObject.parseObject(sourceParam);
        CacheConnectionParam connectionParams = (CacheConnectionParam) JSONUtils.parseObject(connectionParam, CacheConnectionParam.class);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("jdbcUrl", connectionParams.getJdbcUrl());
        jsonObject.put("driver", connectionParams.getDriverClassName());
        jsonObject.put("username", connectionParams.getUsername());
        jsonObject.put("password", PasswordUtils.decodePassword(connectionParams.getPassword()));
        jsonObject.put("table_path", String.format("%s.%s", connectionParams.getDatabase(), sourceParamObj.getString("table_path")));
        jsonObject.put("plugin-type", DbType.CACHE.getCode());
        jsonObject.put("dialect", DbType.CACHE.getCode());
        jsonObject.put("driver_location", connectionParams.getDriverLocation());

        log.info("build source json is " + jsonObject.toJSONString());
        return jsonObject;
    }

    public JSONObject buildSinkJson(String connectionParam, String globalParam) {
        JSONObject globalParamObj = JSONObject.parseObject(globalParam);
        JSONObject sourceObj = globalParamObj.getJSONObject("source");
        JSONObject sinkObj = globalParamObj.getJSONObject("sink");

        JSONObject jsonObject = new JSONObject();
        String sinkTablePath;
        if (sinkObj.getBoolean("autoCreateTable")) {
            sinkTablePath = sourceObj.getString("table_path");
            jsonObject.put("generate_sink_sql", "true");
        } else {
            sinkTablePath = sinkObj.getString("table_path");
            String[] split = sinkTablePath.split("\\.");
            if (split.length == 1) {
                sinkTablePath = "SQLUser." + sinkTablePath;
            }
        }

        CacheConnectionParam connectionParams = (CacheConnectionParam) JSONUtils.parseObject(connectionParam, CacheConnectionParam.class);

        jsonObject.put("jdbcUrl", connectionParams.getJdbcUrl());
        jsonObject.put("driver", connectionParams.getDriverClassName());
        jsonObject.put("username", connectionParams.getUsername());
        jsonObject.put("password", PasswordUtils.decodePassword(connectionParams.getPassword()));
        jsonObject.put("table", sinkTablePath);
        jsonObject.put("plugin-type", DbType.CACHE.getCode());
        jsonObject.put("dialect", DbType.CACHE.getCode());
        jsonObject.put("database", connectionParams.getDatabase());
        jsonObject.put("driver_location", connectionParams.getDriverLocation());
        jsonObject.put("enable_upsert", sinkObj.getBoolean("enable_upsert"));
        jsonObject.put("data_save_mode", sinkObj.getString("data_save_mode"));
        jsonObject.put("batch_size", sinkObj.getString("batch_size"));

        log.info("build sink json is " + jsonObject.toJSONString());
        return jsonObject;
    }
}
