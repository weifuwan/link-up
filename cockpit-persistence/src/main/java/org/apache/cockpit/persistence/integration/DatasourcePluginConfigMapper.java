package org.apache.cockpit.persistence.integration;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.cockpit.common.bean.po.integration.DataSourcePluginConfigPO;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface DatasourcePluginConfigMapper extends BaseMapper<DataSourcePluginConfigPO> {

}
