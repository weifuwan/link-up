package org.apache.cockpit.persistence.metrics;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.cockpit.common.bean.po.metrics.MetricsJvmPO;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface MetricsJvmMapper extends BaseMapper<MetricsJvmPO> {

}
