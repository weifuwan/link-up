import React from 'react';
import { LatestData } from './types';


interface LatestMetricsProps {
  latestData: LatestData;
}

const LatestMetrics: React.FC<LatestMetricsProps> = ({ latestData }) => {
  const cards = [
    { title: '内存使用率', value: latestData?.heapMemoryUsage || 0, unit: '%' },
    { title: 'Full GC次数', value: latestData?.gcFullCount || 0, unit: '次' },
    { title: '阻塞线程', value: latestData?.threadBlockedCount || 0, unit: '个' },
    { title: '最新收集时间', value: latestData?.collectTime || "-", unit: "时间" },
  ];

  return (
    <div
      style={{
        margin: '16px 16px 0 16px',
        padding: 12,
        background: 'white',
        overflowX: 'auto',
      }}
    >
      <div
        style={{
          display: 'flex',
          gap: '16px',
          flexWrap: 'nowrap',
          width: '100%',
        }}
      >
        {cards.map((item, index) => (
          <div key={index} style={{ flex: '1', width: '100%' }}>
            <div className="css-1qqgizd">
              <div className="title">{item.title}</div>
              <div className="big-number">{item.value || 0}</div>
              <div>
                <div className="small-number">单位：{item.unit}</div>
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default LatestMetrics;