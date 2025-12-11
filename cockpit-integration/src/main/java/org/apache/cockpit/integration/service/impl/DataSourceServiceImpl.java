package org.apache.cockpit.integration.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.dto.integration.DataSourceDTO;
import org.apache.cockpit.common.bean.entity.result.PaginationResult;
import org.apache.cockpit.common.bean.po.integration.DataSourcePO;
import org.apache.cockpit.common.bean.vo.integration.DataSourceVO;
import org.apache.cockpit.common.bean.vo.tree.OptionVO;
import org.apache.cockpit.common.enums.integration.ConnStatus;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.utils.ConvertUtil;
import org.apache.cockpit.common.utils.JSONUtils;
import org.apache.cockpit.integration.service.DataSourceService;
import org.apache.cockpit.integration.service.TaskDefinitionService;
import org.apache.cockpit.persistence.integration.DataSourceMapper;
import org.apache.cockpit.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.cockpit.plugin.datasource.api.datasource.DataSourceProcessor;
import org.apache.cockpit.plugin.datasource.api.utils.DataSourceUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class DataSourceServiceImpl extends ServiceImpl<DataSourceMapper, DataSourcePO> implements DataSourceService {

    @Resource
    private TaskDefinitionService taskDefinitionService;

    @Override
    public DataSourceVO create(DataSourceDTO dto) {

        BaseDataSourceParamDTO dataSourceParam = DataSourceUtils.buildDatasourceParam(dto.getConnectionParams());
        DataSourceUtils.checkDatasourceParam(dataSourceParam);
        if (checkName(dto.getDbName())) {
            throw new RuntimeException("Data source name already exists");
        }
        ConnectionParam connectionParam = DataSourceUtils.buildConnectionParams(dataSourceParam);
        DataSourcePO dataSource = ConvertUtil.sourceToTarget(dto, DataSourcePO.class);
        dataSource.setConnectionParams(JSONUtils.toJsonString(connectionParam));
        dataSource.setOriginalJson(dto.getConnectionParams());
        dataSource.setConnStatus(ConnStatus.CONNECTED_NONE);
        dataSource.initInsert();
        try {
            save(dataSource);
            return ConvertUtil.sourceToTarget(dataSource, DataSourceVO.class);
        } catch (DuplicateKeyException ex) {
            throw new RuntimeException("Failed to insert data source");
        }
    }

    @Override
    public String update(String id, DataSourceDTO dto) {
        DataSourceVO dataSourceVO = selectById(id);
        if (dataSourceVO == null) {
            throw new RuntimeException("Data source does not exist");
        }

        BaseDataSourceParamDTO dataSourceParam = DataSourceUtils.buildDatasourceParam(dto.getConnectionParams());
        DataSourceUtils.checkDatasourceParam(dataSourceParam);
        ConnectionParam connectionParam = DataSourceUtils.buildConnectionParams(dataSourceParam);
        DataSourcePO dataSource = ConvertUtil.sourceToTarget(dto, DataSourcePO.class);
        dataSource.setConnectionParams(JSONUtils.toJsonString(connectionParam));
        dataSource.setOriginalJson(dto.getConnectionParams());
        dataSource.setId(id);
        dataSource.initUpdate();
        try {
            updateById(dataSource);
            return id;
        } catch (DuplicateKeyException ex) {
            throw new RuntimeException("Failed to update data source");
        }
    }

    @Override
    public DataSourceVO selectById(String id) {
        return ConvertUtil.sourceToTarget(getById(id), DataSourceVO.class);
    }

    @Override
    public PaginationResult<DataSourceVO> paging(DataSourceDTO dto) {
        LambdaQueryWrapper<DataSourcePO> wrapper = buildWrapper(dto);

        IPage<DataSourcePO> iPage = new Page<>(dto.getPageNo(), dto.getPageSize());

        IPage<DataSourcePO> result = page(iPage, wrapper);

        List<DataSourceVO> records = ConvertUtil.sourceListToTarget(result.getRecords(), DataSourceVO.class);

        List<DataSourceVO> res = records.stream().peek(item -> {
            JSONObject jsonObject = JSON.parseObject(item.getConnectionParams());
            String jdbcUrl = jsonObject.getString("jdbcUrl");
            item.setJdbcUrl(jdbcUrl);
            item.setEnvironmentName(item.getEnvironment().getDescription());
        }).collect(Collectors.toList());

        return PaginationResult.buildSuc(res, iPage);
    }

    @Override
    public Boolean delete(String id) {
        if (StringUtils.isBlank(id)) {
            throw new RuntimeException("Data source ID is null");
        }
        // Check if there are associated sync tasks
        if (taskDefinitionService.existDataSource(id) || taskDefinitionService.existDataSink(id)) {
            throw new RuntimeException("Delete failed, there are associated data sync tasks");
        }

        return removeById(id);
    }

    @Override
    public Boolean connectionTest(String id) {
        DataSourcePO dataSource = getById(id);
        if (dataSource == null) {
            throw new RuntimeException("Data source does not exist");
        }

        // Update connection status to CONNECTING
        updateConnectionStatus(dataSource, ConnStatus.CONNECTING);

        try {
            Boolean isConnected = checkConnection(
                    dataSource.getDbType(),
                    DataSourceUtils.buildConnectionParams(
                            dataSource.getDbType(),
                            dataSource.getConnectionParams()
                    )
            );

            if (isConnected) {
                updateConnectionStatus(dataSource, ConnStatus.CONNECTED_SUCCESS);
            }
            return isConnected;

        } catch (Exception e) {
            log.error("Connection test failed for data source: {}", id, e);
            updateConnectionStatus(dataSource, ConnStatus.CONNECTED_FAILED);
            return false;
        }
    }


    @Override
    public List<OptionVO> option(String dbType) {
        LambdaQueryWrapper<DataSourcePO> queryWrapper = new LambdaQueryWrapper<>();

        if (dbType != null) {
            queryWrapper.eq(DataSourcePO::getDbType, dbType);
        }

        List<DataSourcePO> dataSourcePOS = getBaseMapper().selectList(queryWrapper);
        return dataSourcePOS.stream().map(item -> {
            OptionVO optionVO = new OptionVO();
            optionVO.setValue(item.getId());
            optionVO.setLabel(item.getDbName());
            return optionVO;
        }).collect(Collectors.toList());
    }

    @Override
    public boolean batchDelete(List<String> ids) {
        if (ids == null || ids.isEmpty()) {
            throw new IllegalArgumentException("Data source ID list cannot be empty");
        }

        try {
            for (String id : ids) {
                delete(id);
            }
            log.info("Batch delete data sources successful, total deleted: {}", ids.size());
            return true;
        } catch (Exception e) {
            log.error("Batch delete data sources failed: {}", e.getMessage(), e);
            throw new RuntimeException("Batch delete data sources failed", e);
        }
    }

    @Override
    public Boolean batchConnectionTest(List<String> ids) {
        if (ids == null || ids.isEmpty()) {
            throw new IllegalArgumentException("Data source ID list cannot be empty");
        }

        for (String id : ids) {
            try {
                // Call single connection test logic
                boolean testResult = connectionTest(id);
                if (!testResult) {
                    log.error("Data source {} connection test failed", id);
                    return false;
                }
                log.debug("Data source {} connection test result: {}", id, testResult);
            } catch (Exception e) {
                log.error("Data source {} connection test exception: {}", id, e.getMessage());
                return false;
            }
        }

        log.info("Batch connection test completed, all {} data sources passed", ids.size());
        return true;
    }

    @Override
    public Boolean connectionTestWithParam(String connJson) {
        BaseDataSourceParamDTO dataSourceParam = DataSourceUtils.buildDatasourceParam(connJson);
        DbType dbType = dataSourceParam.getType();
        if (dbType == null) {
            throw new RuntimeException("dbType is null");
        }
        DataSourceUtils.checkDatasourceParam(dataSourceParam);
        ConnectionParam connectionParam = DataSourceUtils.buildConnectionParams(dataSourceParam);
        try {
            return checkConnection(
                    dbType,
                    connectionParam
            );

        } catch (Exception e) {
            log.error("Connection test failed", e);
            return false;
        }
    }

    /**
     * Connection test
     *
     * @param dbType          Data source type
     * @param connectionParam Connection parameters
     * @return Returns true if successful, otherwise false
     */
    public Boolean checkConnection(DbType dbType, ConnectionParam connectionParam) {
        DataSourceProcessor sshDataSourceProcessor = DataSourceUtils.getDatasourceProcessor(dbType);
        Boolean connectivity = sshDataSourceProcessor.checkDataSourceConnectivity(connectionParam);
        if (connectivity) {
            return connectivity;
        }
        throw new RuntimeException("Data source connection test failed");
    }

    private LambdaQueryWrapper<DataSourcePO> buildWrapper(DataSourceDTO dto) {
        LambdaQueryWrapper<DataSourcePO> wrapper = new LambdaQueryWrapper<>();

        if (StringUtils.isNoneBlank(dto.getDbName())) {
            wrapper.eq(DataSourcePO::getDbName, dto.getDbName());
        }

        if (dto.getDbType() != null) {
            wrapper.eq(DataSourcePO::getDbType, dto.getDbType());
        }

        if (dto.getEnvironment() != null) {
            wrapper.eq(DataSourcePO::getEnvironment, dto.getEnvironment());
        }
        return wrapper;
    }

    private boolean checkName(String dbName) {
        LambdaQueryWrapper<DataSourcePO> wrapper = new LambdaQueryWrapper<>();
        wrapper.eq(DataSourcePO::getDbName, dbName.trim());
        List<DataSourcePO> queryDataSource = getBaseMapper().selectList(wrapper);
        return queryDataSource != null && !queryDataSource.isEmpty();
    }


    /**
     * Helper method to update connection status
     */
    private void updateConnectionStatus(DataSourcePO dataSource, ConnStatus status) {
        dataSource.setConnStatus(status);
        updateById(dataSource);
    }
}