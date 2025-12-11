import { Button, Col, DatePicker, Form, Row, Select, Space, theme } from 'antd';
import moment from 'moment';
import { useState } from 'react';
import DatabaseIcons from '../data-source/icon/DatabaseIcons';

interface AdvancedSearchFormProps {
  onSearch: (values: any) => void;
  onReset: () => void;
}
const { RangePicker } = DatePicker;
const AdvancedSearchForm: React.FC<AdvancedSearchFormProps> = ({ onSearch, onReset }) => {
  const { token } = theme.useToken();
  const [form] = Form.useForm();
  const [expand, setExpand] = useState(false);

  const formStyle: React.CSSProperties = {
    maxWidth: 'none',
    background: 'white',
    borderRadius: token.borderRadiusLG,
    padding: 24,
  };

  const handleFinish = (values: any) => {
    onSearch(values);
  };

  const handleReset = () => {
    form.resetFields();
    onReset();
  };

  const createDataSourceOption = (dbType: any, value: any) => ({
    label: (
      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
        <DatabaseIcons dbType={dbType} width={'14'} height={'14'} />
        {dbType}
      </div>
    ),
    value: value || dbType.toUpperCase(),
  });

  const dataSourceOption = [
    createDataSourceOption('MySql', 'MYSQL'),
    createDataSourceOption('Oracle', 'ORACLE'),
    createDataSourceOption('PostgreSql', 'POSTGRESQL'),
    createDataSourceOption('SQLServer', 'SQLSERVER'),
    // createDataSourceOption('SQLITE', 'SQLITE'),
    createDataSourceOption('MongoDB', 'MONGODB'),
    // createDataSourceOption('DB2', 'DB2'),
    createDataSourceOption('Cache', 'CACHE'),
    // createDataSourceOption('Elasticsearch', 'ELASTICSEARCH'),
    createDataSourceOption('OpenGauss', 'OPENGUASS'),
    createDataSourceOption('Doris', 'DORIS'),
    createDataSourceOption('HIVE3', 'HIVE3'),
    createDataSourceOption('StarRocks', 'STARROCKS'),
    createDataSourceOption('ClickHouse', 'CLICKHOUSE'),
    createDataSourceOption('DaMeng', 'DAMENG'),
  ];

  const defaultTimeRange = [moment().subtract(4, 'days'), moment().add(1, 'days')];

  const initialValues = {
    createTime: defaultTimeRange,
  };

  return (
    <Form
      form={form}
      name="advanced_search"
      style={formStyle}
      onFinish={handleFinish}
      initialValues={initialValues}
    >
      <Row gutter={24}>
        <Col span={6} key="1">
          <Form.Item name="createTime" label="创建时间">
            <RangePicker size="small" style={{ width: '100%' }} />
          </Form.Item>
        </Col>
        <Col span={4} key="2">
          <Form.Item name="status" label="任务状态">
            <Select
              placeholder="请选择任务执行状态"
              size="small"
              allowClear
              options={[
                {
                  label: '运行中',
                  value: 'RUNNING',
                },
                {
                  label: '已完成',
                  value: 'COMPLETED',
                },
                {
                  label: '失败',
                  value: 'FAILED',
                },
              ]}
            />
          </Form.Item>
        </Col>
        <Col span={4} key="3">
          <Form.Item name="sourceType" label="数据来源">
            <Select
              placeholder="请选择数据来源"
              size="small"
              options={dataSourceOption}
              allowClear
              showSearch
            />
          </Form.Item>
        </Col>
        <Col span={4} key="4">
          <Form.Item name="sinkType" label="数据去向">
            <Select
              placeholder="请选择数据去向"
              size="small"
              options={dataSourceOption}
              allowClear
              showSearch
            />
          </Form.Item>
        </Col>

        <Col span={6} key="5" style={{ paddingTop: 4 }}>
          <div style={{ display: 'flex', alignItems: 'center' }}>
            <Space size="small">
              <Button type="primary" htmlType="submit" size="small" style={{ width: 70 }}>
                搜索
              </Button>
              <Button onClick={handleReset} size="small" style={{ width: 70 }}>
                重置
              </Button>
              {/* <a style={{ fontSize: 12 }} onClick={() => setExpand(!expand)}>
                <DownOutlined rotate={expand ? 180 : 0} /> 展开
              </a> */}
            </Space>
          </div>
        </Col>
      </Row>
    </Form>
  );
};

export default AdvancedSearchForm;
