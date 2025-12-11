package org.apache.cockpit.integration.service;

import com.baomidou.mybatisplus.extension.service.IService;
import org.apache.cockpit.common.bean.po.integration.DataSourcePluginConfigPO;
import org.apache.cockpit.common.form.PluginConfigResponse;

public interface DatasourcePluginService extends IService<DataSourcePluginConfigPO> {

    PluginConfigResponse getPluginConfig(String pluginName);
}
