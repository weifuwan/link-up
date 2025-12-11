package org.apache.cockpit.integration.service;

import com.baomidou.mybatisplus.extension.service.IService;
import org.apache.cockpit.common.bean.dto.integration.DataSourceDTO;
import org.apache.cockpit.common.bean.entity.result.PaginationResult;
import org.apache.cockpit.common.bean.po.integration.DataSourcePO;
import org.apache.cockpit.common.bean.vo.integration.DataSourceVO;
import org.apache.cockpit.common.bean.vo.tree.OptionVO;

import java.util.List;

public interface DataSourceService extends IService<DataSourcePO> {

    /**
     * Create data source
     */
    DataSourceVO create(DataSourceDTO dto);

    /**
     * Update data source
     */
    String update(String id, DataSourceDTO dto);

    /**
     * Get data source by ID
     */
    DataSourceVO selectById(String id);

    /**
     * Pagination query for data sources
     */
    PaginationResult<DataSourceVO> paging(DataSourceDTO dto);

    /**
     * Delete data source
     */
    Boolean delete(String id);

    /**
     * Connection test for data source
     * 数据源连接测试
     *
     * @param id Data source ID
     * @return Connection test result
     */
    Boolean connectionTest(String id);

    /**
     * Get data source options by database type
     */
    List<OptionVO> option(String dbType);

    /**
     * Batch delete data sources
     *
     * @param ids List of data source IDs
     * @return Delete result
     */
    boolean batchDelete(List<String> ids);

    /**
     * Batch connection test for data sources
     *
     * @param ids List of data source IDs
     * @return List of connection test results
     */
    Boolean batchConnectionTest(List<String> ids);

    Boolean connectionTestWithParam(String connJson);
}