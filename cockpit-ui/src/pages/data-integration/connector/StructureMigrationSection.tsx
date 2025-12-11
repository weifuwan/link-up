import React from 'react';
import { Table } from 'antd';
import Header from '@/components/Header';
import { TableInfo } from './type';


interface TaskDetailPanelProps {
  item: string;
}

const StructureMigrationSection: React.FC<TaskDetailPanelProps> = ({ item }) => {
  const tableData: TableInfo[] = [
    {
      sourceDatabase: 'dmp',
      sourceTable: 't_app_test',
      targetTable: 't_app_test_back',
      method: 'test',
      ddl: "CREATE TABLE `t_dc_indicator_derive` (`id` varchar(32) NOT NULL COMMENT 'id', `time_granularity_id` varchar(32) NOT NULL COMMENT '时间周期',",
    },
  ];

  const columns = [
    {
      title: '来源库',
      dataIndex: 'sourceDatabase',
      key: 'sourceDatabase',
    },
    {
      title: '来源表',
      dataIndex: 'sourceTable',
      key: 'sourceTable',
    },
    {
      title: '目标表',
      dataIndex: 'targetTable',
      key: 'targetTable',
    },
    {
      title: '目标对接方式',
      dataIndex: 'method',
      key: 'method',
    },
    {
      title: 'DDL',
      dataIndex: 'ddl',
      key: 'ddl',
    },
  ];

  return (
    <div
      style={{
        margin: '16px 16px 164px 16px',
        padding: 16,
        background: '#fff',
        borderRadius: 4,
        boxShadow: '0 2px 6px #0000000d',
      }}
    >
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
        }}
      >
        <Header title={<span style={{ fontSize: 14 }}>结构迁移</span>} />
        <div style={{ width: 150 }}>{/* 预留操作按钮 */}</div>
      </div>

      <div>
        <Table dataSource={tableData} columns={columns} pagination={false} />
      </div>
    </div>
  );
};

export default StructureMigrationSection;