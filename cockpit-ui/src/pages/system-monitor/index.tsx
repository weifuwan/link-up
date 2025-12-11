import { message } from "antd";
import { useCallback, useEffect, useRef, useState } from "react";
import ChartsContainer from "./ChartsContainer";
import LatestMetrics from "./LatestMetrics";
import Header from "./Header";
import { LatestData, latestMetricsApi, TaskType, TimeRange } from "./types";

const App: React.FC = () => {
  const [memoryChartData, setMemoryChartData] = useState<any>({});
  const [threadChartData, setThreadChartData] = useState<any>({});
  const [loading, setLoading] = useState(true);
  const [latestData, setLatestData] = useState<LatestData>({
    heapMemoryUsage: 0,
    gcFullCount: 0,
    threadBlockedCount: 0,
    collectTime: "",
  });

  const [taskType, setTaskType] = useState<TaskType>('JVM');
  const [timeRange, setTimeRange] = useState<TimeRange>('24h');

  // 使用 useCallback 避免函数重复创建
  const calculateTimeRange = useCallback(() => {
    const now = new Date();
    const getFormattedTime = (date: Date) => {
      const year = date.getFullYear();
      const month = String(date.getMonth() + 1).padStart(2, '0');
      const day = String(date.getDate()).padStart(2, '0');
      const hours = String(date.getHours()).padStart(2, '0');
      const minutes = String(date.getMinutes()).padStart(2, '0');
      const seconds = String(date.getSeconds()).padStart(2, '0');
      return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
    };

    const startTime = new Date();
    switch (timeRange) {
      case '24h':
        startTime.setDate(now.getDate() - 1);
        break;
      case '48h':
        startTime.setDate(now.getDate() - 2);
        break;
      case 'week':
        startTime.setDate(now.getDate() - 7);
        break;
      default:
        startTime.setHours(now.getHours() - 1);
    }

    return {
      startTime: getFormattedTime(startTime),
      endTime: getFormattedTime(now),
    };
  }, [timeRange]);

  // 使用 useCallback 包装 refreshData 函数
  const refreshData = useCallback(async () => {
    try {
      setLoading(true);
      const { startTime, endTime } = calculateTimeRange();
      
      // 并行请求数据
      const [memoryResponse, threadResponse, latestResponse] = await Promise.all([
        latestMetricsApi.getMemoryUsage(startTime, endTime),
        latestMetricsApi.getThreadUsage(startTime, endTime),
        latestMetricsApi.latest()
      ]);

      // 一次性更新所有状态，减少渲染次数
      setMemoryChartData(memoryResponse?.data || {});
      setThreadChartData(threadResponse?.data || {});
      setLatestData(latestResponse?.data || {
        heapMemoryUsage: 0,
        gcFullCount: 0,
        threadBlockedCount: 0,
        successTasks: 0,
      });
    } catch (error: any) {
      message.error(error.message);
    } finally {
      setLoading(false);
    }
  }, [calculateTimeRange]);

  // 使用 useRef 跟踪是否是第一次渲染
  const isFirstRender = useRef(true);

  useEffect(() => {
    // 如果是第一次渲染，直接执行
    if (isFirstRender.current) {
      refreshData();
      isFirstRender.current = false;
      return;
    }

    // 后续只有 timeRange 或 taskType 变化时才执行
    refreshData();
  }, [timeRange, taskType, refreshData]);

  // 防抖处理时间范围变化
  const handleTimeRangeChange = useCallback((newTimeRange: any) => {
    setTimeRange(newTimeRange);
  }, []);

  const handleTaskTypeChange = useCallback((newTaskType: any) => {
    setTaskType(newTaskType);
  }, []);

  return (
    <div style={{ overflow: 'hidden' }}>
      <Header
        timeRange={timeRange}
        taskType={taskType}
        onTimeRangeChange={handleTimeRangeChange}
        onTaskTypeChange={handleTaskTypeChange}
      />

      <div style={{ overflowY: 'auto', height: 'calc(100vh - 135px)' }}>
        <LatestMetrics latestData={latestData}/>
        <div className="di-process-summary-container">
          {!loading && memoryChartData?.series && (
            <ChartsContainer chartData={memoryChartData} title={'JVM内存'} />
          )}
          
          {!loading && threadChartData?.series && (
            <ChartsContainer chartData={threadChartData} title={'线程'} />
          )}
        </div>
      </div>
    </div>
  );
};

export default App;