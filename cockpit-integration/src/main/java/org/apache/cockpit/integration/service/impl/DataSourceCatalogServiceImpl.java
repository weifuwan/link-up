package org.apache.cockpit.integration.service.impl;

import org.apache.cockpit.common.bean.entity.integration.QueryResult;
import org.apache.cockpit.common.bean.vo.integration.DataSourceVO;
import org.apache.cockpit.common.bean.vo.tree.ColumnOptionVO;
import org.apache.cockpit.common.bean.vo.tree.OptionVO;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.integration.service.DataSourceCatalogService;
import org.apache.cockpit.integration.service.DataSourceService;
import org.apache.cockpit.plugin.datasource.api.modal.DataSourceTableColumn;
import org.apache.cockpit.plugin.datasource.api.utils.DataSourceUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class DataSourceCatalogServiceImpl implements DataSourceCatalogService {

    @Resource
    private DataSourceService dataSourceService;

    @Override
    public List<OptionVO> listTable(String id) {
        DataSourceVO dataSourceVO = dataSourceService.selectById(id);
        if (dataSourceVO == null) {
            throw new RuntimeException("数据源不存在");
        }
        ConnectionParam connectionParam = DataSourceUtils.buildConnectionParams(dataSourceVO.getDbType(), dataSourceVO.getConnectionParams());
        List<String> tables = DataSourceUtils.getDatasourceProcessor(dataSourceVO.getDbType())
                .listTables(connectionParam);
        return tables.stream().map(item -> {
            OptionVO optionVO = new OptionVO();
            optionVO.setLabel(item);
            optionVO.setValue(item);
            return optionVO;
        }).collect(Collectors.toList());
    }

    @Override
    public List<ColumnOptionVO> listColumn(String id, String tableName) {
        DataSourceVO dataSourceVO = dataSourceService.selectById(id);
        if (dataSourceVO == null) {
            throw new RuntimeException("数据源不存在");
        }
        ConnectionParam connectionParam = DataSourceUtils.buildConnectionParams(dataSourceVO.getDbType(), dataSourceVO.getConnectionParams());
        List<DataSourceTableColumn> tables = DataSourceUtils.getDatasourceProcessor(dataSourceVO.getDbType())
                .listColumns(connectionParam, tableName);
        return tables.stream().map(item -> {
            ColumnOptionVO optionVO = new ColumnOptionVO();
            optionVO.setLabel(item.getColumnName());
            optionVO.setValue(item.getColumnName());
            optionVO.setType(item.getColumnType());
            optionVO.setSourceType(item.getSourceType());
            return optionVO;
        }).collect(Collectors.toList());
    }

    @Override
    public QueryResult getTop20Data(String datasourceId, Map<String, Object> requestBody) {
        DataSourceVO dataSourceVO = dataSourceService.selectById(datasourceId);
        if (dataSourceVO == null) {
            throw new RuntimeException("数据源不存在");
        }
        ConnectionParam connectionParam = DataSourceUtils.buildConnectionParams(dataSourceVO.getDbType(), dataSourceVO.getConnectionParams());

        return DataSourceUtils.getDatasourceProcessor(dataSourceVO.getDbType())
                .getTop20Data(connectionParam, requestBody);
    }
}
