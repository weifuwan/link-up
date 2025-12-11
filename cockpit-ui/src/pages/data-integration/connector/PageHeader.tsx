import { ArrowLeftOutlined } from '@ant-design/icons';
import { Breadcrumb } from 'antd';
import React from 'react';

interface PageHeaderProps {
  onBack: () => void;
  title: string;
  breadcrumbItems?: Array<{ title: string | React.ReactNode }>;
}

export const PageHeader: React.FC<PageHeaderProps> = ({
  onBack,
  title,
  breadcrumbItems = [
    { title: <span style={{ fontSize: 12 }}>任务列表</span> },
    { title: <span style={{ fontSize: 12 }}>新建同步任务</span> },
  ],
}) => {
  return (
    <div style={{ padding: '12px 24px 6px', backgroundColor: 'white', boxShadow: "0 5px 11px rgba(0, 0, 0, 0.11)", zIndex: 300 }}>
      <Breadcrumb items={breadcrumbItems} />
      <div style={{ display: 'flex', alignItems: 'center', fontSize: 15, marginTop: 12 }}>
        <ArrowLeftOutlined onClick={onBack} style={{ cursor: 'pointer' }} />
        <span style={{ marginLeft: 12, height: 24, lineHeight: '24px' }}>{title}</span>
      </div>
    </div>
  );
};