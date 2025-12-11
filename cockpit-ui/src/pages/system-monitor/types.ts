import HttpUtils from "@/utils/HttpUtils";

export interface ChartDataItem {
  date: string;
  value: any;
}

export interface ChartDataSet {
  data: any[];
  xAxis: string[];
}



export interface LatestData {
  heapMemoryUsage: number;
  gcFullCount: number;
  threadBlockedCount: number;
  collectTime: string;
}

export type TimeRange = '24h' | '48h' | 'week';
export type TaskType = 'JVM';

export const apiPrefixlatestMetrics = "/api/v1/metrics/jvm"

export const latestMetricsApi = {

  // 获取最新的JVM指标
  latest: (): Promise<{ code: number; data: any; message?: string }> => {
    return HttpUtils.get(`${apiPrefixlatestMetrics}/latest?applicationName=cockpit&instanceId=cockpit`);
  },

  getMemoryUsage: (startTime: string, endTime: string): Promise<{ code: number; data: any; message?: string }> => {
    return HttpUtils.get(`${apiPrefixlatestMetrics}/memory-usage?applicationName=cockpit&startTime=${startTime}&endTime=${endTime}`);
  },

  getThreadUsage: (startTime: string, endTime: string): Promise<{ code: number; data: any; message?: string }> => {
    return HttpUtils.get(`${apiPrefixlatestMetrics}/thread-usage?applicationName=cockpit&startTime=${startTime}&endTime=${endTime}`);
  },
};