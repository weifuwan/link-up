import { DownOutlined } from '@ant-design/icons';
import { Dropdown, Modal, Popconfirm, Space, message } from 'antd';
// import CreateModal from '../modal/CreateModal';
import { useRef } from 'react';
import TaskViewModal from './TaskViewModal';
import { taskDefinitionApi, taskExecutionApi, taskScheduleApi } from './type';

interface ActionColumnProps {
  record: any;
  menuItems: any[];
  cbk: () => void;
}

const { confirm } = Modal;

const ActionColumn: React.FC<ActionColumnProps> = ({ record, menuItems, cbk }) => {
  const ref = useRef<any>(null);

  const handleExecute = () => {
    taskExecutionApi.execute(record?.id).then((data) => {
      if (data?.code === 0) {
        message.success('提交成功');
        cbk();
      } else {
        message.error(data?.message);
      }
    });
  };

  const handleStop = () => {
    const executionId = record?.executionId;
    if (executionId !== undefined) {
      taskExecutionApi.cancel(executionId).then((data) => {
        if (data?.code === 0) {
          message.success('提交成功');
          cbk();
        } else {
          message.error(data?.message);
        }
      });
    }
  };

  const handleDeleteTask = async (record: any) => {
    confirm({
      title: '确认要删除么？',
      centered: true,
      content: (
        <span>
          您确定要删除任务 [{<span style={{ color: 'orange' }}> {record.name} </span>}
          ] 吗？ <br />
          任务一旦删除将无法恢复，请谨慎操作。
        </span>
      ),
      okText: '删除',
      okType: 'primary',
      okButtonProps: {
        size: 'small',
        danger: true,
      },
      cancelButtonProps: {
        size: 'small',
      },
      maskClosable: true,
      onOk() {
        if (record?.id) {
          doDeleteDataSource(record?.id);
        } else {
          message.error('id不存在');
        }
      },
    });
  };

  const doDeleteDataSource = async (id: string) => {
    const response = await taskDefinitionApi.delete(id);
    if (response.code === 0) {
      message.success(response.message);
      cbk();
    } else {
      message.error(response.message);
    }
  };

  const handleMenuClick = (info: any) => {
    info.domEvent.stopPropagation();
    console.log(record);
    if (info?.key === 'delete') {
      handleDeleteTask(record);
    } else if (info?.key === 'view') {
      ref.current.onOpen(true, record, cbk);
    } else if (info?.key === '3') {
    } else if (info?.key === '4') {
    }
  };

  return (
    <>
      <Space size="middle">
        {record?.status === 'RUNNING' ? (
          <Popconfirm
            title="停止任务"
            description={<div style={{ marginRight: 12 }}>确定要停止这个任务吗？</div>}
            okText="Yes"
            cancelText="No"
            onConfirm={handleStop}
          >
            <a>停止</a>
          </Popconfirm>
        ) : (
          <Popconfirm
            title="启动任务"
            description={<div style={{ marginRight: 12 }}>确定要启动这个任务吗？</div>}
            okText="Yes"
            cancelText="No"
            onConfirm={handleExecute}
          >
            <a>启动</a>
          </Popconfirm>
        )}

        {record?.scheduleStatus === 'RUNNING' ? (
          <Popconfirm
            title="调度任务"
            description={<div style={{ marginRight: 12 }}>确定要下线调度任务吗？</div>}
            okText="Yes"
            cancelText="No"
            onConfirm={async () => {
              if (record?.taskScheduleId) {
                const response = await taskScheduleApi.stopSchedule(record?.taskScheduleId);
                if (response?.code === 0) {
                  cbk();
                  message.success('下线成功');
                } else {
                  message.error(response?.message);
                }
              } else {
                message.error('任务调度ID不存在');
              }
            }}
          >
            <a>调度下线</a>
          </Popconfirm>
        ) : (
          <Popconfirm
            title="调度任务"
            description={<div style={{ marginRight: 12 }}>确定要上线调度任务吗？</div>}
            okText="Yes"
            cancelText="No"
            onConfirm={async () => {
              if (record?.taskScheduleId) {
                const response = await taskScheduleApi.startSchedule(record?.taskScheduleId);
                if (response?.code === 0) {
                  cbk();
                  message.success('上线成功');
                } else {
                  message.error(response?.message);
                }
              } else {
                message.error('任务调度ID不存在');
              }
            }}
          >
            <a>调度上线</a>
          </Popconfirm>
        )}

        <Dropdown
          menu={{
            items: menuItems.map((menuItem) => ({
              ...menuItem,
              onClick: handleMenuClick,
            })),
          }}
        >
          <a>
            更多 <DownOutlined style={{ fontSize: 12 }} />
          </a>
        </Dropdown>
        <TaskViewModal ref={ref} />
      </Space>
    </>
  );
};

export default ActionColumn;
