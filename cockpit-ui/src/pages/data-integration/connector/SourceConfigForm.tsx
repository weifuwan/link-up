import { TableOutlined } from '@ant-design/icons';
import { Button, Col, Form, message, Popover, Radio, Row, Select } from 'antd';
import TextArea from 'antd/es/input/TextArea';
import React, { useRef, useState } from 'react';
import DatabaseIcons from '../data-source/icon/DatabaseIcons';
import { dataSourceCatalogApi } from '../data-source/type';
import QualityDetail from './DataViewSQL';
import { FilterConditions } from './FilterConditions';
import { FilterTimeConditions } from './FilterTimeConditions';

interface SourceConfigFormProps {
  form: any;
  basicForm: any;
  sourceType: string;
  sourceOption: any[];
  sourceTableOption: any[];
  filterConditions: any[];
  filterTimeConditions: any[];
  onFilterConditionsChange: (conditions: any[]) => void;
  onFilterTimeConditionsChange: (conditions: any[]) => void;
  onSourceChange: (value: string) => void;
  sourceId: string;
  setReaderColumns: (columns: any[]) => void;
  selectedType: string;
  filterType: string;
  setFilterType: (type: any) => void;
}

export const SourceConfigForm: React.FC<SourceConfigFormProps> = ({
  form,
  basicForm,
  sourceType,
  sourceOption,
  sourceTableOption,
  filterConditions,
  filterTimeConditions,
  onFilterConditionsChange,
  onFilterTimeConditionsChange,
  onSourceChange,
  sourceId,
  setReaderColumns,
  selectedType,
  filterType,
  setFilterType
}) => {
  const [fieldOptions, setFieldOptions] = useState<any[]>([]);
  const [fieldTimeOptions, setFieldTimeOptions] = useState<any[]>([]);
  const ref = useRef<any>(null);
  const [open, setOpen] = useState(false);
  const [viewLoading, setViewLoading] = useState(false);
  const [countLoading, setCountLoading] = useState(false);
  

  const getTop20Data = () => {
    const sourceId = form?.getFieldValue('sourceId');
    if (sourceId === undefined || sourceId === '') {
      message.warning('请选择数据源');
    } else {
      const taskExecuteType = basicForm?.getFieldValue('taskExecuteType');
      const table_path = form?.getFieldValue('table_path');
      const query = form?.getFieldValue('query');
      setViewLoading(true);
      dataSourceCatalogApi
        .getTop20Data(sourceId, {
          taskExecuteType: taskExecuteType,
          table_path: table_path || '',
          query: query || '',
        })
        .then((data) => {
          if (data?.code === 0) {
            ref.current?.onOpen(true, data);
            setViewLoading(false);
          } else {
            message.error(data?.message);
            setViewLoading(false);
          }
        });
    }
  };

  const handleOpenChange = (newOpen: boolean) => {
    setCountLoading(true);
    setOpen(newOpen);
    setCountLoading(false);
  };

  const renderSource = () => {
    if (selectedType === 'SINGLE_TABLE') {
      return (
        <>
          <Form.Item label="表名" name="table_path" rules={[{ required: true }]}>
            <Select
              prefix={<TableOutlined style={{ color: 'orange' }} />}
              size="small"
              placeholder="请输入表名"
              allowClear
              onChange={(value) => {
                dataSourceCatalogApi.listColumn(sourceId, value).then((data) => {
                  if (data?.code === 0) {
                    const res = data?.data;
                    setReaderColumns(res);
                    setFieldOptions(res);
                    // 过滤时间字段
                    const fileterd = res?.filter((item) => {
                      return item?.type === 'TIMESTAMP';
                    });

                    setFieldTimeOptions(fileterd || []);
                  } else {
                    message.error(data?.message);
                  }
                });
              }}
              options={sourceTableOption || []}
              showSearch
            />
          </Form.Item>

          <div style={{ fontSize: 12, marginBottom: 8, marginTop: 4 }}>过滤方式: </div>
          <div style={{ marginBottom: 4 }}>
            <Radio.Group value={filterType} onChange={(e) => setFilterType(e.target.value)}>
              <Radio value="data">数据过滤</Radio>
              <Radio value="time">时间增量过滤</Radio>
            </Radio.Group>
          </div>
          {filterType === 'data' && (
            <Form.Item label="过滤条件" name="filterConditions">
              <FilterConditions
                conditions={filterConditions}
                onConditionsChange={onFilterConditionsChange}
                fieldOptions={fieldOptions}
              />
            </Form.Item>
          )}
          {filterType === 'time' && (
            <Form.Item label="时间条件" name="filterConditions">
              <FilterTimeConditions
                conditions={filterTimeConditions}
                onConditionsChange={onFilterTimeConditionsChange}
                fieldOptions={fieldTimeOptions}
              />
            </Form.Item>
          )}
        </>
      );
    } else if (selectedType === 'SINGLE_TABLE_CUSTOM') {
      return (
        <>
          <Form.Item label="自定义查询" name="query" rules={[{ required: true }]}>
            <TextArea rows={9} maxLength={40000} />
          </Form.Item>
        </>
      );
    } else {
      <>good</>;
    }
  };

  return (
    <>
      <Form form={form} layout="vertical">
        <Form.Item label="数据源" name="sourceId" rules={[{ required: true }]}>
          <Select
            prefix={<DatabaseIcons dbType={sourceType} width="14" height="14" />}
            size="small"
            onChange={(value) => {
              onSourceChange(value);
            }}
            placeholder="请选择数据源"
            options={sourceOption || []}
          />
        </Form.Item>

        {renderSource()}

        <Form.Item label="" name="">
          <Row gutter={24} justify="space-between">
            <Col span={12}>
              <Button
                style={{ width: '100%', marginTop: 12 }}
                type="primary"
                onClick={getTop20Data}
                loading={viewLoading}
              >
                数据预览
              </Button>
            </Col>
            <Col span={12}>
              <Popover
                title="数据统计"
                content={
                  <div>
                    数据总量：<span style={{ color: 'blue' }}>9527 嘿嘿</span>
                  </div>
                }
                trigger="click"
                open={open}
                onOpenChange={handleOpenChange}
              >
                <Button
                  style={{ width: '100%', marginTop: 12 }}
                  type="default"
                  loading={countLoading}
                >
                  数据统计
                </Button>
              </Popover>
            </Col>
          </Row>
        </Form.Item>
      </Form>

      <QualityDetail ref={ref} />
    </>
  );
};
