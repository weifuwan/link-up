import * as echarts from 'echarts';
import React, { useEffect, useRef } from 'react';

interface LineSeries {
  name: string;
  type?: string;
  stack?: string;
  data: any[];
  smooth?: boolean;
  symbol?: string;
  lineStyle?: Record<string, any>;
  areaStyle?: Record<string, any>;
}

interface LineChartProps {
  title: string;
  xAxisData: string[];
  series: LineSeries[];
  unit?: string;
  height?: number | string;
  grid?: {
    top?: string | number;
    right?: string | number;
    bottom?: string | number;
    left?: string | number;
  };
  showLegend?: boolean;
  yAxisName?: string;
  xAxisInterval?: number | 'auto' | ((index: number, value: string) => boolean);
  xAxisRotate?: number;
  dataZoom?:
    | boolean
    | {
        type?: 'inside' | 'slider';
        start?: number;
        end?: number;
      };
}

const MultiLineChart: React.FC<LineChartProps> = ({
  title,
  xAxisData,
  series,
  unit = '',
  height = 400,
  grid = { top: '10%', right: '2%', bottom: '0%', left: '3%' },
  showLegend = false,
  yAxisName = '',
  xAxisInterval = 'auto',
  xAxisRotate = 0,
  dataZoom = false,
}) => {
  const chartRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!chartRef.current) return;

    const chart = echarts.init(chartRef.current);

    // 定义颜色数组
    const colorPalette = [
      '#5470c6',
      '#91cc75',
      '#fac858',
      '#ee6666',
      '#73c0de',
      '#3ba272',
      '#fc8452',
      '#9a60b4',
      '#ea7ccc',
      '#5470c6',
      '#91cc75',
      '#fac858',
    ];

    // 强制只显示开始和结束标签
    const getXAxisLabel = (index: number, value: string) => {
      // 如果是第一个或最后一个数据点，显示标签
      if (index === 0 || index === xAxisData.length - 1) {
        return value;
      }
      // 其他情况不显示标签
      return '';
    };

    const option: echarts.EChartsOption = {
      title: {
        text: title,
        left: 'left',
        textStyle: {
          color: '#333',
          fontSize: 16,
          fontWeight: 'bold',
          fontFamily: 'Arial, sans-serif',
        },
      },
      color: colorPalette,
      tooltip: {
        trigger: 'axis',
        axisPointer: {
          type: 'cross',
          label: {
            backgroundColor: '#6a7985',
          },
        },
        // // 确保tooltip中显示完整的时间信息
        // formatter: (params: any) => {
        //   const dataIndex = params[0].dataIndex;
        //   const time = xAxisData[dataIndex];
        //   let result = `${time}<br/>`;
        //   params.forEach((item: any) => {
        //     const value = unit ? `${item.value} ${unit}` : item.value;
        //     result += `${item.marker} ${item.seriesName}: ${value}<br/>`;
        //   });
        //   return result;
        // },
      },
      legend: showLegend
        ? {
            data: series.map((s) => s.name),
            top: '0%',
            textStyle: {
              fontSize: 12,
            },
          }
        : undefined,
      grid: {
        containLabel: true,
        left: grid.left,
        right: grid.right,
        bottom: dataZoom ? '18%' : grid.bottom,
        top: grid.top,
      },
      xAxis: {
        type: 'category',
        data: xAxisData,
        boundaryGap: false,
        axisLabel: {
          rotate: xAxisRotate,
          fontSize: 11,
          // 强制只显示开始和结束标签
          formatter: getXAxisLabel,
          // 或者使用更简单的方式：只显示第一个和最后一个
          // interval: (index: number) => index === 0 || index === xAxisData.length - 1,
        },
        axisLine: {
          lineStyle: {
            color: '#ccc',
          },
        },
        // 确保轴线完整显示
        axisTick: {
          alignWithLabel: true,
          // 也只显示开始和结束的刻度
          interval: (index: number) => index === 0 || index === xAxisData.length - 1,
        },
      },
      yAxis: {
        type: 'value',
        name: yAxisName,
        nameTextStyle: {
          fontSize: 12,
        },
        axisLabel: {
          formatter: unit ? `{value} ${unit}` : '{value}',
          fontSize: 11,
        },
        axisLine: {
          lineStyle: {
            color: '#ccc',
          },
        },
        splitLine: {
          lineStyle: {
            type: 'dashed',
            color: '#e6e6e6',
          },
        },
      },
      dataZoom: dataZoom
        ? [
            {
              type: dataZoom === true ? 'inside' : dataZoom.type || 'inside',
              start: dataZoom === true ? 0 : dataZoom.start || 0,
              end: dataZoom === true ? 100 : dataZoom.end || 100,
            },
            {
              type: 'slider',
              show: true,
              start: dataZoom === true ? 0 : dataZoom.start || 0,
              end: dataZoom === true ? 100 : dataZoom.end || 100,
              bottom: '0%',
              height: 15,
            },
          ]
        : undefined,
      series: series.map((item, index) => ({
        name: item.name,
        type: item.type || 'line',
        stack: item.stack,
        data: item.data,
        smooth: true,
        symbol: item.symbol || 'circle',
        symbolSize: 4,
        lineStyle: item.lineStyle || {
          width: 2,
        },
        showSymbol: false,
        areaStyle: item.areaStyle || {
          color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
            { offset: 0, color: colorPalette[index] + '99' },
            { offset: 1, color: colorPalette[index] + '10' },
          ]),
        },
        emphasis: {
          focus: 'series',
          itemStyle: {
            borderWidth: 2,
            borderColor: '#000',
          },
        },
      })),
      animation: true,
      animationDuration: 1000,
      animationEasing: 'cubicOut',
    };

    chart.setOption(option);

    // 响应式调整
    const handleResize = () => chart.resize();
    window.addEventListener('resize', handleResize);

    // 清理函数
    return () => {
      window.removeEventListener('resize', handleResize);
      chart.dispose();
    };
  }, [
    title,
    xAxisData,
    series,
    unit,
    grid,
    showLegend,
    yAxisName,
    xAxisInterval,
    xAxisRotate,
    dataZoom,
  ]);

  return (
    <div
      ref={chartRef}
      style={{
        width: '100%',
        height: height,
        minHeight: '300px',
      }}
    />
  );
};

export default MultiLineChart;