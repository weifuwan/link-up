import { Form, Input, Select } from 'antd';
import TextArea from 'antd/es/input/TextArea';
import { useEffect } from 'react';
import DataSourceSelector from './DataSourceSelector';
import IconRightArrow from './IconRightArrow';
import { TASK_TYPE_OPTIONS } from './taskTypesConfig';

type TaskBasicInfoFormProps = {
  form: any;
  sourceType: any;
  sinkType: any;
  onTaskTypeChange: (value: string) => void;
  onSourceChange: (value: any) => void;
  onTargetChange: (value: any) => void;
};

const TaskBasicInfoForm = ({
  form,
  sourceType,
  sinkType,
  onTaskTypeChange,
  onSourceChange,
  onTargetChange,
}: TaskBasicInfoFormProps) => {
  const useTaskGenerator = (sourceType: any, targetType: any) => {
    const now = new Date();
    const dateStr = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, '0')}${String(
      now.getDate(),
    ).padStart(2, '0')}`;
    const timeStr = `${String(now.getHours()).padStart(2, '0')}${String(now.getMinutes()).padStart(
      2,
      '0',
    )}${String(now.getSeconds()).padStart(2, '0')}`;

    return `sync_${sourceType.toLowerCase()}_to_${targetType.toLowerCase()}_${dateStr}_${timeStr}`;
  };

  useEffect(() => {
    form.setFieldsValue({
      name: useTaskGenerator(sourceType, sinkType),
      taskExecuteType: 'SINGLE_TABLE',
    });
  }, [sourceType]);

  return (
    <Form labelCol={{ span: 2 }} wrapperCol={{ span: 20 }} form={form}>
      {/* 数据来源与去向 */}
      <Form.Item label="数据来源与去向" name="dsName">
        <div style={{ display: 'flex' }}>
          <DataSourceSelector
            type="source"
            value={sourceType}
            onChange={onSourceChange}
            style={{ width: '49%' }}
          />

          <div style={{ display: 'flex', alignItems: 'center', margin: '0 8px' }}>
            <IconRightArrow />
          </div>

          <DataSourceSelector
            type="target"
            value={sinkType}
            onChange={onTargetChange}
            style={{ width: '49%' }}
          />
        </div>
      </Form.Item>

      {/* 任务名称 */}
      <Form.Item
        label="新任务名称"
        name="name"
        rules={[
          { required: true, message: '请输入任务名称' },
          { max: 100, message: '任务名称不能超过100个字符' },
        ]}
      >
        <Input placeholder="新任务名称" maxLength={100} size="small" allowClear />
      </Form.Item>

      {/* 同步类型 */}
      <Form.Item
        label="同步类型"
        name="taskExecuteType"
        rules={[{ required: true, message: '请选择同步类型' }]}
      >
        <Select
          size="small"
          options={TASK_TYPE_OPTIONS}
          onChange={onTaskTypeChange}
          placeholder="请选择同步类型"
        />
      </Form.Item>

      {/* 任务描述 */}
      <Form.Item
        label="任务描述"
        name="remark"
        rules={[{ max: 1024, message: '描述不能超过1024个字符' }]}
      >
        <TextArea showCount rows={4} placeholder="任务描述" size="small" maxLength={1024} />
      </Form.Item>
    </Form>
  );
};

export default TaskBasicInfoForm;
