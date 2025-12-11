import { ChartDataItem, TimeRange } from './types';

export const formatDate = (dateStr: string, timeRange: TimeRange): string => {
  // 如果已经是格式化好的时间（如 '13:00'），在 24h 模式下直接返回
  if (timeRange === '24h' && /^\d{1,2}:\d{2}$/.test(dateStr)) {
    return dateStr;
  }
  
  // 否则按日期解析
  const date = new Date(dateStr);
  
  if (isNaN(date.getTime())) {
    console.warn(`Invalid date string: ${dateStr}`);
    return dateStr;
  }
  
  switch (timeRange) {
    case 'week':
      return `${date.getMonth() + 1}月${date.getDate()}日`;
    case '48h':
      return `${date.getMonth() + 1}月${date.getDate()}日 ${date.getHours().toString().padStart(2, '0')}:00`;
    case '24h':
    default:
      // 对于 24h 模式，如果传入的是完整日期，只取小时
      return `${date.getHours().toString().padStart(2, '0')}:00`;
  }
};

export const transformChartData = (trendData: ChartDataItem[], timeRange: TimeRange) => {
  return {
    data: trendData.map((item) => item.value),
    xAxis: trendData.map((item) => formatDate(item.date, timeRange)),
  };
};

export const timeRangeMap = {
  '最近一周': 'week' as TimeRange,
  '最近48小时': '48h' as TimeRange,
  '最近24小时': '24h' as TimeRange,
};

export const taskTypeOptions = [
  {
    label: 'JVM内存',
    value: 'JVM',
  },
];