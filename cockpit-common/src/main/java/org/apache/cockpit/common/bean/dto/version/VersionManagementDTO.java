package org.apache.cockpit.common.bean.dto.version;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * 智能建模 - 版本管理
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class VersionManagementDTO  {

    /**
     * 主键
     */
    private String id;

    /**
     * 原表中记录的ID
     */
    private String sourceTableId;

    /**
     * 版本数据
     */
    private String versionData;

    /**
     * 版本类型
     */
    private String versionType;

    /**
     * 版本号
     */
    private Integer versionNumber;

    /**
     * 创建时间
     */
    private Date createTime;

    /**
     * 创建人
     */
    private String createBy;

    /**
     * 更新时间
     */
    private Date updateTime;

    /**
     * 更新人
     */
    private String updateBy;

}