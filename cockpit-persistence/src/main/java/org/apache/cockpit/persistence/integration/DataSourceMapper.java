package org.apache.cockpit.persistence.integration;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.cockpit.common.bean.po.integration.DataSourcePO;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface DataSourceMapper extends BaseMapper<DataSourcePO> {

}
