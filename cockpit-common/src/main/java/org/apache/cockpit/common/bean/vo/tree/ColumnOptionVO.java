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
public class ColumnOptionVO {

    @ApiModelProperty("value")
    private Object value;

    @ApiModelProperty("label")
    private Object label;

    @ApiModelProperty("type")
    private Object type;

    @ApiModelProperty("sourceType")
    private Object sourceType;
}