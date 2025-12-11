package org.apache.cockpit.plugin.datasource.dm.catalog;

import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.utils.JSONUtils;
import org.apache.cockpit.common.constant.Constant;
import org.apache.cockpit.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.cockpit.plugin.datasource.api.constants.DataSourceConstants;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.dm.param.DmConnectionParam;
import org.apache.cockpit.plugin.datasource.dm.param.DmDataSourceParamDTO;
import org.apache.commons.lang.StringUtils;

public class DmParamConverter {

    public static BaseDataSourceParamDTO createDatasourceParamDTO(String connectionJson) {
        DmConnectionParam connectionParams = JSONUtils.parseObject(connectionJson, DmConnectionParam.class);

        DmDataSourceParamDTO damengDatasourceParamDTO = new DmDataSourceParamDTO();

        damengDatasourceParamDTO.setUsername(connectionParams.getUsername());
        damengDatasourceParamDTO.setDatabase(connectionParams.getDatabase());
        damengDatasourceParamDTO.setOther(connectionParams.getOther());

        String address = connectionParams.getAddress();
        String[] hostSeperator = address.split(Constant.DOUBLE_SLASH);
        String[] hostPortArray = hostSeperator[hostSeperator.length - 1].split(Constant.COMMA);
        damengDatasourceParamDTO.setPort(Integer.parseInt(hostPortArray[0].split(Constant.COLON)[1]));
        damengDatasourceParamDTO.setHost(hostPortArray[0].split(Constant.COLON)[0]);

        return damengDatasourceParamDTO;
    }

    public static BaseConnectionParam createConnectionParams(BaseDataSourceParamDTO datasourceParam) {
        DmDataSourceParamDTO dmDatasourceParam = (DmDataSourceParamDTO) datasourceParam;

        String address = String
                .format("%s%s:%s", DataSourceConstants.JDBC_DAMENG, dmDatasourceParam.getHost(),
                        dmDatasourceParam.getPort());
        String jdbcUrl = StringUtils.isEmpty(dmDatasourceParam.getUsername()) ? address
                : String.format("%s/%s", address,
                dmDatasourceParam.getUsername());

        DmConnectionParam dmConnectionParam = new DmConnectionParam();
        dmConnectionParam.setUsername(dmDatasourceParam.getUsername());
        dmConnectionParam.setPassword(PasswordUtils.encodePassword(dmDatasourceParam.getPassword()));
        dmConnectionParam.setAddress(address);
        dmConnectionParam.setJdbcUrl(jdbcUrl);
        dmConnectionParam.setDatabase(dmDatasourceParam.getDatabase());
        dmConnectionParam.setDriverClassName(DataSourceConstants.COM_DAMENG_JDBC_DRIVER);
        dmConnectionParam.setDriverLocation(dmDatasourceParam.getDriverLocation());
        dmConnectionParam.setValidationQuery(DataSourceConstants.DAMENG_VALIDATION_QUERY);
        dmConnectionParam.setDbType(DbType.DAMENG);
        dmConnectionParam.setOther(dmDatasourceParam.getOther());

        return dmConnectionParam;
    }
}