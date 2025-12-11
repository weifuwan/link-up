package org.apache.cockpit.common.bean.vo.tree;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@ApiModel
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OptionEnhanceVO {

    @ApiModelProperty("value")
    private Object value;

    @ApiModelProperty("label")
    private Object label;

    @ApiModelProperty("title")
    private String title;

    @ApiModelProperty("enName")
    private String enName;
}