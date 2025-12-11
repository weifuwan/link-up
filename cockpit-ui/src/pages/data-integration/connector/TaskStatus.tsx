import {
  CheckCircleFilled,
  CloseOutlined,
  Loading3QuartersOutlined,
  MacCommandOutlined,
} from '@ant-design/icons';

interface TaskStatusProps {
  status: string;
}

const TaskStatus: React.FC<TaskStatusProps> = ({ status }) => {
  const renderStatus = (status: string) => {
    if (status === 'COMPLETED') {
      return (
        <span style={{ color: 'green' }}>
          <CheckCircleFilled style={{ marginRight: 8 }} /> 成功
        </span>
      );
    } else if (status === 'RUNNING') {
      return (
        <span style={{ color: 'blue' }}>
          <Loading3QuartersOutlined spin style={{ marginRight: 8 }} /> <span>运行中</span>
        </span>
      );
    } else if (status === 'FAILED') {
      return (
        <span style={{ color: 'red' }}>
          <CloseOutlined style={{ marginRight: 8, fontSize: '90%' }} /> <span>失败</span>
        </span>
      );
    } else {
      return (
        <span>
          <MacCommandOutlined style={{ marginRight: 8 }} /> 未启动
        </span>
      );
    }
  };

  return <div>{renderStatus(status)}</div>;
};

export default TaskStatus;
