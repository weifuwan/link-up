import { SyncOutlined } from '@ant-design/icons';
import type { TransferProps } from 'antd';
import { Col, Form, Input, message, Row, Select, Transfer } from 'antd';
import { useForm } from 'antd/es/form/Form';
import React, { useState } from 'react';
import { dataSourceCatalogApi } from '../data-source/type';
import './index.less';

interface RecordType {
  key: string;
  title: any;
  chosen: boolean;
}

const App: React.FC = ({ sourceOption, setMultiTableList, multiTableList, sinkOption, form }) => {
  const [data, setData] = useState<RecordType[]>([]);
  const handleChange: TransferProps['onChange'] = (newTargetKeys) => {
    setMultiTableList(newTargetKeys);
  };

  const renderFooter: TransferProps['footer'] = (_, info) => {
    if (info?.direction === 'left') {
      return (
        <div>
          <span style={{ fontSize: 12, padding: '8px 12px' }}>1/1 个库 1/5 个表</span>
        </div>
      );
    }
    return (
      <div>
        <span style={{ fontSize: 12, padding: '8px 12px' }}>2/4 个表</span>
        <span style={{ color: 'red', fontSize: 12, padding: '8px 12px' }}>最多支持100个表</span>
      </div>
    );
  };

  return (
    <>
      <Form form={form} initialValues={{}}>
        <Row gutter={24} style={{ marginBottom: 4 }}>
          <Col span={12}>
            <div style={{ fontSize: 15, fontWeight: 600 }}>
              源端库表
              <span style={{ marginLeft: 8, cursor: 'pointer', color: 'blue' }}>
                <SyncOutlined />
              </span>
            </div>
            <Form.Item
              label="库选择"
              name="sourceId"
              rules={[{ required: true }]}
              labelCol={{ span: 3 }}
              wrapperCol={{ span: 21 }}
              className="custom-form-item"
            >
              <Select
                options={sourceOption || []}
                size="small"
                style={{ width: '99%' }}
                placeholder="请选择库名"
                onChange={(value) => {
                  dataSourceCatalogApi.listTable(value).then((data) => {
                    if (data?.code === 0) {
                      const res = data?.data;
                      const tmp = res?.map((item: any) => {
                        return {
                          key: item?.value,
                          title: item?.label,
                          chosen: false,
                        };
                      });
                      setData(tmp);
                    } else {
                      message.error(data?.message);
                    }
                  });
                }}
              />
            </Form.Item>
            <Form.Item
              label="表过滤"
              name="sourceTable"
              rules={[{ required: true }]}
              labelCol={{ span: 3 }}
              wrapperCol={{ span: 21 }}
              className="custom-form-item"
            >
              <Input size="small" style={{ width: '99%' }} placeholder="请输入表名或者正则" />
            </Form.Item>
          </Col>
          <Col span={12}>
            <div style={{ fontSize: 15, fontWeight: 600 }}>已选库表</div>
            <Form.Item
              label="库选择"
              name="sinkId"
              rules={[{ required: true }]}
              labelCol={{ span: 3 }}
              wrapperCol={{ span: 21 }}
              className="custom-form-item"
            >
              <Select
                options={sinkOption || []}
                size="small"
                style={{ width: '99%' }}
                placeholder="请选择库名"
              />
            </Form.Item>
          </Col>
        </Row>
      </Form>

      <Transfer
        dataSource={data}
        listStyle={{
          width: '100%',
          height: 400,
        }}
        operations={['', '']}
        targetKeys={multiTableList}
        onChange={handleChange}
        render={(item) => item.title}
        footer={renderFooter}
      />
    </>
  );
};

export default App;
