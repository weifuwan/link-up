interface ExecutionStatusProps {
  record: any;
}
// <Progress percent={40} percentPosition={{ align: 'center', type: 'inner' }} size={[300, 20]} />
const ExecutionStatus: React.FC<ExecutionStatusProps> = ({ record }) => {
  return (
    <>
      <div style={{ display: 'flex', alignItems: 'center' }}>
        <span style={{ fontWeight: 700, fontSize: 19, marginRight: 8 }}>·</span>
        <span style={{ marginRight: 16 }}>执行方式：</span>
        <span style={{ color: 'gray' }}>
          {record?.executionMode === 'MANUAL' ? '手动执行' : '调度执行'}
        </span>
      </div>
      <div style={{ display: 'flex', alignItems: 'center' }}>
        <span style={{ fontWeight: 700, fontSize: 19, marginRight: 8 }}>·</span>
        <span style={{ marginRight: 16 }}>同步时间：</span>
        <span style={{ color: 'gray' }}>{record?.duration || '-'}</span>
      </div>
      <div style={{ display: 'flex', alignItems: 'center' }}>
        <span style={{ fontWeight: 700, fontSize: 19, marginRight: 8 }}>·</span>
        <span style={{ marginRight: 16 }}>同步条数：</span>
        <span style={{ color: 'gray' }}>
          {record?.sinkTotalRecord !== undefined ? record?.sinkTotalRecord || '0' : '0'} 条
        </span>
      </div>
      <div style={{ display: 'flex', alignItems: 'center' }}>
        <span style={{ fontWeight: 700, fontSize: 19, marginRight: 8 }}>·</span>
        <span style={{ marginRight: 42 }}>QPS：</span>
        <span style={{ color: 'gray' }}>{record?.qps + ' 条/秒' || '-'}</span>
      </div>
      <div style={{ display: 'flex', alignItems: 'center' }}>
        <span style={{ fontWeight: 700, fontSize: 19, marginRight: 8 }}>·</span>
        <span style={{ marginRight: 16 }}>同步大小：</span>
        <span style={{ color: 'gray' }}>{record?.syncSize || '-'}</span>
      </div>
    </>
  );
};

export default ExecutionStatus;
