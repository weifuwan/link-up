package org.apache.cockpit.integration.service;

import org.apache.cockpit.common.bean.entity.integration.QueryResult;
import org.apache.cockpit.common.bean.vo.tree.ColumnOptionVO;
import org.apache.cockpit.common.bean.vo.tree.OptionVO;

import java.util.List;
import java.util.Map;

public interface DataSourceCatalogService {

    List<OptionVO> listTable(String id);

    List<ColumnOptionVO> listColumn(String id, String tableName);

    QueryResult getTop20Data(String datasourceId, Map<String, Object> requestBody);

}
