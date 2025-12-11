import { Divider, Table, message } from 'antd';
import { TableRowSelection } from 'antd/es/table/interface';
import moment from 'moment';
import { useEffect, useState } from 'react';
import ActionColumn from './ActionColumn';
import AdvancedSearchForm from './AdvancedSearchForm';
import BottomActionBar from './BottomActionBar';
import DataSourceSyncPlan from './DataSourceSyncPlan';
import ExecutionStatus from './ExecutionStatus';
import Footer from './Footer';
import './index.less';
import ScheduleInfo from './ScheduleInfo';
import TaskStatus from './TaskStatus';
import { taskDefinitionApi, taskExecutionApi } from './type';

const App: React.FC = () => {
  const [taskList, setTaskList] = useState([]);
  const [searchParams, setSearchParams] = useState({});
  const [pagination, setPagination] = useState({
    current: 1,
    pageSize: 10,
    total: 0,
  });
  const [loading, setLoading] = useState(false);

  const [selectedRowKeys, setSelectedRowKeys] = useState<React.Key[]>([]);
  useEffect(() => {
    fetchTaskList();
  }, [searchParams, pagination.current, pagination.pageSize]);

  const fetchTaskList = () => {
    setLoading(true);
    // 在发送请求前转换参数
    const transformedParams = { ...searchParams };

    if (transformedParams?.createTime) {
      transformedParams.startTime = moment(transformedParams.createTime[0]).format(
        'YYYY-MM-DD HH:mm:ss',
      );
      transformedParams.endTime = moment(transformedParams.createTime[1]).format(
        'YYYY-MM-DD HH:mm:ss',
      );
      delete transformedParams.createTime;
    }

    taskDefinitionApi.page({ ...transformedParams }).then((data) => {
      if (data?.code === 0) {
        setTaskList(data?.data?.bizData);
        setPagination((prev) => ({
          ...prev,
          total: data?.data?.pagination?.total || 0,
        }));
        setLoading(false);
      } else {
        message.error(data?.message);
        setLoading(false);
      }
    });
  };

  const menuItems = [
    { key: 'view', label: '查看' },
    { key: 'delete', label: '删除' },
  ];

  const baseColumns = [
    {
      title: '任务名称',
      dataIndex: 'name',
      width: '15%',
      ellipsis: true,
    },
    {
      title: '数据源同步方案',
      dataIndex: 'taskName',
      width: '20%',
      render: (content: any, record: any) => <DataSourceSyncPlan record={record} />,
    },
    {
      title: '执行状态',
      dataIndex: 'taskParams',
      width: '7%',
      render: (content: any, record: any) => <TaskStatus status={record?.status} />,
    },
    {
      title: '执行情况',
      dataIndex: '执行情况',
      width: '15%',
      render: (content: any, record: any) => <ExecutionStatus record={record} />,
    },
    {
      title: '调度情况',
      dataIndex: 'taskName',
      width: '20%',
      render: (content: any, record: any) => <ScheduleInfo record={record} />,
    },
    {
      title: '创建时间',
      dataIndex: 'createTime',
      width: '10%',
    },
    {
      title: '操作',
      dataIndex: '',
      width: '17%',
      fixed: 'right',
      render: (record: any) => (
        <ActionColumn record={record} menuItems={menuItems} cbk={fetchTaskList} />
      ),
    },
  ];

  const onSelectChange = (newSelectedRowKeys: React.Key[]) => {
    setSelectedRowKeys(newSelectedRowKeys);
  };

  const rowSelection: TableRowSelection<any> = {
    selectedRowKeys,
    onChange: onSelectChange,
  };

  const handleSearch = (values: any) => {
    setSearchParams(values);
    setPagination((prev) => ({ ...prev, current: 1 }));
  };

  const handleReset = () => {
    setSearchParams({});
  };

  const handlePaginationChange = (page: number, pageSize: number) => {
    setPagination((prev) => ({ ...prev, current: page, pageSize }));
  };

  const hasSelected = selectedRowKeys.length > 0;

  const onStartAll = () => {
    taskExecutionApi.batchExecute(selectedRowKeys).then((data) => {
      if (data?.code === 0) {
        message.success('全部启动成功');
        setSelectedRowKeys([]);
        fetchTaskList();
      } else {
        message.error('全部启动失败');
      }
    });
  };

  const onStopAll = () => {
    taskExecutionApi.batchCancel(selectedRowKeys).then((data) => {
      if (data?.code === 0) {
        message.success('全部启动成功');
        setSelectedRowKeys([]);
        fetchTaskList();
      } else {
        message.error('全部启动失败');
      }
    });
  };

  return (
    <>
      <div
        style={{
          margin: 16,
          background: 'white',
          padding: 16,
        }}
      >
        <div>
          <div className="config-manage-page">
            <div className="operate-bar">
              <div className="left">
                <AdvancedSearchForm onSearch={handleSearch} onReset={handleReset} />
              </div>
            </div>
            <Divider style={{ margin: '0 0 24px' }} />
            <Table
              columns={baseColumns as any}
              dataSource={taskList}
              rowKey={'id'}
              bordered
              pagination={false}
              loading={loading}
              rowSelection={{ type: 'checkbox', ...rowSelection }}
              scroll={{ x: 'max-content', y: 'calc(100vh - 450px)' }}
            />
          </div>
        </div>
        {taskList && taskList?.length > 1 ? '' : <Footer />}
      </div>

      <BottomActionBar
        onStart={onStartAll}
        onStop={onStopAll}
        pagination={{
          ...pagination,
          onChange: handlePaginationChange,
        }}
        disabled={!hasSelected}
      />
    </>
  );
};

export default App;
