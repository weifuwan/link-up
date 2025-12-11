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
public class TreeSelectVO {

    @ApiModelProperty("key")
    private String key;

    @ApiModelProperty("value")
    private String value;

    @ApiModelProperty("title")
    private String title;

    @ApiModelProperty("parentId")
    private String parentId;

    @ApiModelProperty("children")
    private List<TreeSelectVO> children;
}