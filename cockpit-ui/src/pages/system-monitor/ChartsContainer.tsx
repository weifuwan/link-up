import React from 'react';

import MultiLineChart from './MultiLineChart';
import "./index.less";

interface ChartsContainerProps {
  chartData: any;
  title: string;
}

const ChartsContainer: React.FC<ChartsContainerProps> = ({ chartData, title }) => {
  return (
    <div key={1} className="echart-container col-2">
      <div className="dc-loading dc-loading-inline" style={{ width: '100%' }}>
        <div className="dc-loading-wrap" style={{ width: '100%', padding: 16 }}>
          <MultiLineChart
            title={title}
            xAxisData={chartData.xaxis}
            series={chartData.series}
            unit="MB"
            xAxisInterval={5}
            dataZoom={{
              type: 'slider',
              start: 0,
              end: 100,
            }}
            height={"50vh"}
          />
        </div>
      </div>
    </div>
  );
};

export default ChartsContainer;
