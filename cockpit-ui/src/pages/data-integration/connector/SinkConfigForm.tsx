import { Col, Form, Input, InputNumber, message, Row, Select, Switch } from 'antd';
import React, { useState } from 'react';
import DatabaseIcons from '../data-source/icon/DatabaseIcons';
import { dataSourceCatalogApi } from '../data-source/type';

interface SinkConfigFormProps {
  form: any;
  sinkType: string;
  sinkOption: any[];
  sinkTableOption: any[];
  autoCreateTable: boolean;
  onAutoCreateTableChange: (checked: boolean) => void;
  onSinkTableChange: (sinkId: string) => void;
  setWriterColumns: (columns: any[]) => void;
  sinkId: any;
  selectedType: string;
}

export const SinkConfigForm: React.FC<SinkConfigFormProps> = ({
  form,
  sinkType,
  sinkOption,
  sinkTableOption,
  autoCreateTable,
  onAutoCreateTableChange,
  onSinkTableChange,
  setWriterColumns,
  sinkId,
  selectedType,
}) => {
  const [isPartition, setIsPartition] = useState<boolean>(false);
  const renderOtherItem = () => {
    if (sinkType.toUpperCase() === 'HIVE3') {
      return (
        <Row gutter={24}>
          <Col span={12}>
            <Form.Item label="文件格式" name="file_format_type" rules={[{ required: true }]}>
              <Select
                size="small"
                options={[
                  {
                    label: 'TEXT',
                    value: 'TEXT',
                  },
                  {
                    label: 'PARQUET',
                    value: 'PARQUET',
                  },
                  {
                    label: 'ORC',
                    value: 'ORC',
                  },
                ]}
                placeholder="请选择数据源"
              />
            </Form.Item>
          </Col>
          <Col span={12}>
            {isPartition === true ? (
              <Form.Item label="分区字段" name="partition_field" rules={[{ required: true }]}>
                <Input size="small" placeholder="请输入分区字段" />
              </Form.Item>
            ) : (
              ''
            )}
          </Col>
        </Row>
      );
    } else {
      return <></>;
    }
  };
  return (
    <Form
      form={form}
      layout="vertical"
      initialValues={{
        autoCreateTable: true,
        enable_upsert: false,
        data_save_mode: 'APPEND_DATA',
        file_format_type: 'TEXT',
        batch_size: 2000,
      }}
    >
      <Form.Item label="数据源" name="sinkId" rules={[{ required: true }]}>
        <Select
          prefix={<DatabaseIcons dbType={sinkType} width="14" height="14" />}
          size="small"
          options={sinkOption || []}
          onChange={onSinkTableChange}
          placeholder="请选择数据源"
        />
      </Form.Item>

      {selectedType === 'SINGLE_TABLE' ? (
        <Row gutter={24}>
          <Col span={12}>
            <Form.Item label="是否自动建表" name="autoCreateTable" rules={[{ required: true }]}>
              <Switch onChange={onAutoCreateTableChange} checked={autoCreateTable} />
            </Form.Item>
          </Col>
          <Col span={12}>
            {sinkType?.toUpperCase() === 'HIVE3' ? (
              <Form.Item label="是否为分区表" name="isPartitionTable" rules={[{ required: true }]}>
                <Switch
                  onChange={(checked) => {
                    setIsPartition(checked);
                  }}
                  checked={isPartition}
                />
              </Form.Item>
            ) : (
              ''
            )}
          </Col>
        </Row>
      ) : (
        <Form.Item label="表名" name="table" rules={[{ required: true }]}>
          <Select
            size="small"
            placeholder="请输入表名"
            options={sinkTableOption || []}
            showSearch
            allowClear
            onChange={(value) => {
              dataSourceCatalogApi.listColumn(sinkId, value).then((data) => {
                if (data?.code === 0) {
                  setWriterColumns(data?.data);
                } else {
                  message.error(data?.message);
                }
              });
            }}
          />
        </Form.Item>
      )}

      {!autoCreateTable && (
        <Form.Item label="表名" name="table" rules={[{ required: true }]}>
          <Select
            size="small"
            placeholder="请输入表名"
            options={sinkTableOption || []}
            showSearch
            allowClear
            onChange={(value) => {
              dataSourceCatalogApi.listColumn(sinkId, value).then((data) => {
                if (data?.code === 0) {
                  setWriterColumns(data?.data);
                } else {
                  message.error(data?.message);
                }
              });
            }}
          />
        </Form.Item>
      )}

      {renderOtherItem()}

      <Form.Item label="存储模式" name="data_save_mode" rules={[{ required: true }]}>
        <Select
          size="small"
          options={[
            {
              label: 'APPEND_DATA（追加数据）',
              value: 'APPEND_DATA',
            },
            {
              label: 'DROP_DATA（清空表之后再追加数据）',
              value: 'DROP_DATA',
            },
          ]}
        />
      </Form.Item>

      <Form.Item label="批量条数" name="batch_size" rules={[{ required: true }]}>
        <InputNumber size="small" style={{ width: '30%' }} />
      </Form.Item>

      {sinkType.toUpperCase() === 'HIVE3' ? (
        ''
      ) : (
        <Form.Item label="是否开启UPSERT" name="enable_upsert" rules={[{ required: true }]}>
          <Switch />
        </Form.Item>
      )}
    </Form>
  );
};
