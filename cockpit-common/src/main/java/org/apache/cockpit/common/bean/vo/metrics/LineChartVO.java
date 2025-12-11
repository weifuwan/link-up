package org.apache.cockpit.common.bean.vo.metrics;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@ApiModel("折线图完整数据")
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class LineChartVO {

    @ApiModelProperty("X轴数据（时间点）")
    private List<String> xAxis;

    @ApiModelProperty("系列数据")
    private List<LineSeriesVO> series;

    @ApiModelProperty("标题")
    private String title;
}
