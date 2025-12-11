package org.apache.cockpit.common.bean.vo.tree;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.List;


@Data
@ApiModel
@NoArgsConstructor
@AllArgsConstructor
public class TreeTypeTotalVO {

    @ApiModelProperty("key")
    private String key;

    @ApiModelProperty("中文名")
    private String title;

    @ApiModelProperty("parentId")
    private String parentId;

    @ApiModelProperty("type")
    private String type;

    @ApiModelProperty("total")
    private Integer total;

    @ApiModelProperty("leaf")
    private boolean leaf;

    @ApiModelProperty("update_time")
    @JsonFormat(pattern = "yyyy年MM月dd HH:mm:ss", timezone = "GMT+8")
    private Date updateTime;

    @ApiModelProperty("children")
    private List<TreeTypeTotalVO> children;


}