package org.apache.cockpit.common.bean.vo.metrics;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@ApiModel("折线图系列数据")
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class LineSeriesVO {

    @ApiModelProperty("系列名称")
    private String name;

    @ApiModelProperty("图表类型")
    private String type = "line";

    @ApiModelProperty("是否堆叠")
    private String stack;

    @ApiModelProperty("数据数组")
    private List<Object> data;

    @ApiModelProperty("是否平滑曲线")
    private Boolean smooth = false;

    @ApiModelProperty("线条样式")
    private Map<String, Object> lineStyle;

    @ApiModelProperty("区域样式")
    private Map<String, Object> areaStyle;
}