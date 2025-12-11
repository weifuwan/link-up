package org.apache.cockpit.common.bean.vo.tree;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;


@Data
@ApiModel
@NoArgsConstructor
@AllArgsConstructor
public class TreeTypeValueVO {

    @ApiModelProperty("key")
    private String key;

    @ApiModelProperty("中文名")
    private String title;

    @ApiModelProperty("parentId")
    private String parentId;

    @ApiModelProperty("type")
    private String type;

    @ApiModelProperty("children")
    private List<TreeTypeValueVO> children;

    @ApiModelProperty("value")
    private String value;
}