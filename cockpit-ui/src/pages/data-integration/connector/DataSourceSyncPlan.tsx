import { DoubleRightOutlined } from '@ant-design/icons';
import { CSSProperties } from 'react';
import DatabaseIcons from '../data-source/icon/DatabaseIcons';

interface DataSourceSyncPlanProps {
  record: any;
}

const DataSourceSyncPlan: React.FC<DataSourceSyncPlanProps> = ({ record }) => {
  const taskParams = JSON.parse(record?.taskParams);
  const source = taskParams?.source;
  const pluginType = source['plugin-type'];
  const sourceTable = source?.table_path || source?.database + '.' + source?.table || '-';

  const sink = taskParams?.sink;
  const sinkPluginType = sink['plugin-type'];
  const sinkTable = sink?.table || sink?.collection || '-';
  const sinkDatabase = sink?.database || '-';
  let sinkDbAndTable = sinkDatabase + '.' + sinkTable;
  if (sinkPluginType.toUpperCase() === 'HIVE3') {
    sinkDbAndTable = sink?.table_name;
  }

  // 动画样式
  const animatedIconStyle: CSSProperties = {
    fontSize: 10,
    animation: 'float 2s ease-in-out infinite',
  };

  return (
    <div style={{ color: 'rgba(0,0,0,0.74)', fontWeight: 500 }}>
      <style>
        {`
          @keyframes float {
            0%, 100% { transform: translateY(0px) rotate(90deg); }
            50% { transform: translateY(-8px) rotate(90deg); }
          }
        `}
      </style>
      <div>
        <span>{record?.taskExecuteTypeName || '未知'}</span>
      </div>
      <div style={{ margin: '4px 0' }}>
        <div style={{ display: 'flex', alignItems: 'center' }}>
          {pluginType !== undefined ? (
            <>
              <DatabaseIcons dbType={pluginType} width="24" height="24" /> &nbsp;&nbsp;
              <a>{source?.sourceName}</a>
            </>
          ) : (
            ''
          )}
          <span style={{ marginLeft: 4 }}>{''}</span>
          <span style={{ margin: '0 4px', marginRight: 4, color: 'green' }}>·</span>
          {sourceTable}
        </div>

        <div style={{ margin: '8px 0', paddingLeft: 7 }}>
          <DoubleRightOutlined style={animatedIconStyle} />
        </div>

        <div style={{ display: 'flex', alignItems: 'center' }}>
          {sinkPluginType !== undefined ? (
            <>
              <DatabaseIcons dbType={sinkPluginType} width="24" height="24" /> &nbsp;&nbsp;
              <a>{sink?.sinkName}</a>
            </>
          ) : (
            ''
          )}
          <span style={{ marginLeft: 4 }}>{''}</span>
          <span style={{ margin: '0 4px' }}>·</span>
          {sinkDbAndTable}
        </div>
      </div>
    </div>
  );
};

export default DataSourceSyncPlan;
