import { Col, Form, Row, message } from 'antd';
import { useEffect, useState } from 'react';
import { dataSourceApi, dataSourceCatalogApi } from '../data-source/type';
import ColumnItem from './ColumnItem';
import { ConfigSection } from './ConfigSection';
import { FooterActions } from './FooterActions';
import './index.less';
import MultiTableForm from './MultiTableForm';
import { PageHeader } from './PageHeader';
import { ScheduleConfigForm } from './ScheduleConfigForm';
import { SinkConfigForm } from './SinkConfigForm';
import { SourceConfigForm } from './SourceConfigForm';
import SyncTitle from './SyncTitle';
import TaskBasicInfoForm from './TaskBasicInfoForm';
import { taskDefinitionApi } from './type';

// 导入子组件

interface DataSyncHeaderProps {
  setDetail: (value: boolean) => void;
  params: any;
}

const DataSyncCreate: React.FC<DataSyncHeaderProps> = ({ setDetail, params }) => {
  // 状态管理
  const [selectedType, setSelectedType] = useState('SINGLE_TABLE');
  const [form] = Form.useForm();
  const [sourceForm] = Form.useForm();
  const [multiForm] = Form.useForm();
  const [sinkForm] = Form.useForm();
  const [scheduleForm] = Form.useForm();
  const [sourceType, setSourceType] = useState<any>('');
  const [sinkType, setSinkType] = useState<any>('');
  const [sourceOption, setSourceOption] = useState<any[]>([]);
  const [sinkOption, setSinkOption] = useState<any[]>([]);
  const [sourceTableOption, setSourceTableOption] = useState<any[]>([]);
  const [sinkTableOption, setSinkTableOption] = useState<any[]>([]);
  const [filterConditions, setFilterConditions] = useState<any[]>([]);
  const [filterTimeConditions, setFilterTimeConditions] = useState<any[]>([]);
  const [autoCreateTable, setAutoCreateTable] = useState<boolean>(true);
  const [loading, setLoading] = useState(false);
  const [readerColumns, setReaderColumns] = useState<any[]>([]);
  const [writerColumns, setWriterColumns] = useState<any[]>([]);
  const [sinkId, setSinkId] = useState<any>('');
  const [multiTableList, setMultiTableList] = useState<any[]>([]);
  const [filterType, setFilterType] = useState<any>('data');

  useEffect(() => {
    if (selectedType === 'SINGLE_TABLE_CUSTOM') {
      const sinkId = sinkForm.getFieldValue('sinkId');
      if (sinkId) {
        getSinkTableList(sinkId);
      }
    }
  }, [selectedType]);

  // 数据源选项加载
  useEffect(() => {
    if (sourceType) {
      dataSourceApi.option(sourceType).then((data: any) => {
        if (data?.code === 0) {
          setSourceOption(data?.data);
          if (data?.data?.length > 0) {
            const firstOption = data.data[0];
            sourceForm.setFieldValue('sourceId', firstOption.value);
            getSourceTableList(firstOption.value);
          }
        } else {
          message.error(data?.message);
        }
      });
    }
  }, [sourceType]);

  useEffect(() => {
    if (autoCreateTable === true) {
      setWriterColumns(readerColumns);
    }
  }, [autoCreateTable, readerColumns]);

  useEffect(() => {
    if (sinkType) {
      dataSourceApi.option(sinkType).then((data: any) => {
        if (data?.code === 0) {
          setSinkOption(data?.data);
          if (data?.data?.length > 0) {
            const firstOption = data.data[0];
            sinkForm.setFieldValue('sinkId', firstOption.value);
          }
        } else {
          message.error(data?.message);
        }
      });
    }
  }, [sinkType]);

  useEffect(() => {
    if (params) {
      setSourceType(params?.sourceType);
      setSinkType(params?.targetType);
    }
  }, [params]);

  useEffect(() => {
    if (!autoCreateTable) {
      const sinkId = sinkForm.getFieldValue('sinkId');
      if (sinkId) {
        getSinkTableList(sinkId);
      }
    }
  }, [autoCreateTable]);
  const [sourceId, setSourceId] = useState<any>('');
  // 数据处理函数
  const getSourceTableList = (id: string) => {
    dataSourceCatalogApi.listTable(id).then((data) => {
      if (data?.code === 0) {
        setSourceId(id);
        setSourceTableOption(data?.data);
      } else {
        message.error(data?.message);
      }
    });
  };

  const getSinkTableList = (id: string) => {
    dataSourceCatalogApi.listTable(id).then((data) => {
      if (data?.code === 0) {
        setSinkId(id);
        setSinkTableOption(data?.data);
      } else {
        message.error(data?.message);
      }
    });
  };

  const onSinkTableChange = (sinkId: string) => {
    getSinkTableList(sinkId);
  };

  const handleSourceChange = (value: string) => {
    setSourceType(value);
  };

  const handleTargetChange = (value: string) => {
    setSinkType(value);
  };

  const handleTaskTypeChange = (value: string) => {
    setSelectedType(value);
  };

  const handleSubmit = async () => {
    if (selectedType === 'SINGLE_TABLE') {
      sinkForm.validateFields();
      form.validateFields().then((values) => {
        sourceForm.validateFields().then((sourceValues) => {
          sinkForm.validateFields().then((sinkValues) => {
            scheduleForm.validateFields().then((scheduleValues) => {
              setLoading(true);
              const params = {
                ...values,
                sourceType,
                sinkType,
                sourceId: sourceValues?.sourceId,
                sinkId: sinkValues?.sinkId,
                taskParams: JSON.stringify({
                  source: { ...sourceValues,whereType: filterType, filterConditions: filterConditions, filterTimeConditions: filterTimeConditions },
                  sink: sinkValues,
                }),
                taskScheduleDTO: scheduleValues,
              };
              taskDefinitionApi.create(params).then((data) => {
                if (data?.code === 0) {
                  message.success('新增成功');
                  setLoading(false);
                  setDetail(false);
                } else {
                  message.error(data?.message);
                  setDetail(false);
                }
              });
            });
          });
        });
      });
    } else if (selectedType === 'SINGLE_TABLE_CUSTOM') {
      sinkForm.validateFields();
      form.validateFields().then((values) => {
        sourceForm.validateFields().then((sourceValues) => {
          sinkForm.validateFields().then((sinkValues) => {
            scheduleForm.validateFields().then((scheduleValues) => {
              const params = {
                ...values,
                sourceType,
                sinkType,
                sourceId: sourceValues?.sourceId,
                sinkId: sinkValues?.sinkId,
                taskParams: JSON.stringify({
                  source: { ...sourceValues, filterConditions: filterConditions },
                  sink: sinkValues,
                }),
                taskScheduleDTO: scheduleValues,
              };
              console.log(params);
              taskDefinitionApi.create(params).then((data) => {
                if (data?.code === 0) {
                  message.success('新增成功');
                  setDetail(false);
                } else {
                  message.error(data?.message);
                  setDetail(false);
                }
              });
            });
          });
        });
      });
    } else {
      form.validateFields().then((values) => {
        scheduleForm.validateFields().then((scheduleValues) => {
          if (multiTableList?.length === 0) {
            message.warning('表不能选择空');
            return;
          }
          const multiValues = multiForm.getFieldsValue();
          const params = {
            ...values,
            sourceType,
            sinkType,
            sourceId: multiValues?.sourceId,
            sinkId: multiValues?.sinkId,
            taskScheduleDTO: scheduleValues,
            multiTableList: multiTableList,
          };
          taskDefinitionApi.batch(params).then((data) => {
            if (data?.code === 0) {
              message.success('新增成功');
              setDetail(false);
            } else {
              message.error(data?.message);
            }
          });
        });
      });
    }
  };

  const renderSyncSetting = () => {
    if (selectedType === 'SINGLE_TABLE' || selectedType === 'SINGLE_TABLE_CUSTOM') {
      return (
        <>
          <Row gutter={24}>
            <Col span={12}>
              <SourceConfigForm
                form={sourceForm}
                basicForm={form}
                sourceType={sourceType}
                sourceOption={sourceOption}
                sourceTableOption={sourceTableOption}
                filterConditions={filterConditions}
                filterTimeConditions={filterTimeConditions}
                onFilterConditionsChange={setFilterConditions}
                onFilterTimeConditionsChange={setFilterTimeConditions}
                onSourceChange={getSourceTableList}
                sourceId={sourceId}
                setReaderColumns={setReaderColumns}
                selectedType={selectedType}
                filterType={filterType}
                setFilterType={setFilterType}
              />
            </Col>
            <Col span={12}>
              <SinkConfigForm
                form={sinkForm}
                sinkType={sinkType}
                sinkOption={sinkOption}
                sinkTableOption={sinkTableOption}
                autoCreateTable={autoCreateTable}
                onAutoCreateTableChange={setAutoCreateTable}
                onSinkTableChange={onSinkTableChange}
                setWriterColumns={setWriterColumns}
                sinkId={sinkId}
                selectedType={selectedType}
              />
            </Col>
          </Row>
        </>
      );
    } else {
      return (
        <>
          <MultiTableForm
            sourceOption={sourceOption}
            setMultiTableList={setMultiTableList}
            multiTableList={multiTableList}
            sinkOption={sinkOption}
            form={multiForm}
          />
        </>
      );
    }
  };

  return (
    <div>
      <PageHeader onBack={() => setDetail(false)} title="新建同步任务" />
      <div style={{ height: 'calc(100vh - 200px)', overflowY: 'auto' }}>
        <ConfigSection title="基本配置">
          <TaskBasicInfoForm
            form={form}
            sourceType={sourceType}
            sinkType={sinkType}
            onTaskTypeChange={handleTaskTypeChange}
            onSourceChange={handleSourceChange}
            onTargetChange={handleTargetChange}
          />
        </ConfigSection>

        <ConfigSection title="同步配置">
          <SyncTitle />
          {renderSyncSetting()}
        </ConfigSection>

        {selectedType === 'SINGLE_TABLE' ? (
          <ConfigSection title="字段映射">
            <div>
              <SyncTitle />
              <ColumnItem readerItems={readerColumns || []} writerItems={writerColumns || []} />
            </div>
          </ConfigSection>
        ) : (
          ''
        )}

        <ConfigSection title="调度配置">
          <div>
            <ScheduleConfigForm form={scheduleForm} />
          </div>
        </ConfigSection>
      </div>
      <FooterActions onSubmit={handleSubmit} loading={loading} />
    </div>
  );
};

export default DataSyncCreate;
