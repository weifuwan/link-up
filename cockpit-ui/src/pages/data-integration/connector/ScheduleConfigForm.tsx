import { Alert, Button, Divider, Form, Input, message, Popover, Radio } from 'antd';
import React, { useState } from 'react';
import { taskScheduleApi } from './type';

interface ScheduleConfigFormProps {
  form: any;
}

export const ScheduleConfigForm: React.FC<ScheduleConfigFormProps> = ({ form }) => {
  const [cronExpression, setCronExpression] = useState<any[]>([]);
  const [cronValue, setCronValue] = useState('');
  const commonCronExpressions = [
    { label: '每天1点', expression: '0 0 1 * * ?' },
    { label: '每周一12点', expression: '0 0 12 ? * MON' },
    { label: '每隔1小时', expression: '0 0 */1 * * ?' },
    { label: '每隔5分钟', expression: '0 0/5 * * * ?' },
    { label: '每月1号8点', expression: '0 0 8 1 * ?' },
  ];

  // 应用cron表达式到表单
  const applyCronExpression = (expression: string) => {
    form.setFieldValue('cronExpression', expression);
    setCronValue(expression);
    message.success('已应用cron表达式');
  };

  return (
    <>
      <div style={{ width: '100%', textAlign: 'center', marginBottom: 12 }}>
        <Alert
          message={<span style={{ fontSize: 12 }}>调度时区为Asia/Shanghai</span>}
          type="info"
          showIcon
          style={{ padding: '2px 8px', marginBottom: 16, borderRadius: 8 }}
        />
      </div>
      <Form
        form={form}
        layout="vertical"
        initialValues={{
          scheduleStatus: 'STOPPED',
        }}
      >
        <Form.Item
          name="cronExpression"
          label="cron表达式"
          rules={[{ required: true, message: '必须填写cron表达式' }]}
        >
          <Input
            size="small"
            placeholder="请输入cron表达式"
            value={cronValue}
            onChange={(e) => {
              // 手动设置表单值
              setCronValue(e.target.value);
              form.setFieldValue('cronExpression', e.target.value);
            }}
            style={{ width: '50%' }}
          />
          <Popover
            content={
              <div>
                {cronExpression.map((item, index) => (
                  <div key={index}>{item}</div> // 每个数字换行展示
                ))}
              </div>
            }
            title="未来最近5次执行时间"
            trigger="click"
          >
            <a
              onClick={() => {
                // 从表单中获取当前的 cronExpression 值
                const cronValue = form.getFieldValue('cronExpression');
                if (!cronValue) {
                  message.error('请先输入cron表达式');
                  return;
                }

                taskScheduleApi.getLast5ExecutionTimes(cronValue).then((data) => {
                  if (data?.code === 0) {
                    setCronExpression(data?.data || []);
                  } else {
                    message.error(data?.message);
                  }
                });
              }}
              style={{ fontSize: 12, marginLeft: 8 }}
              type="text"
            >
              最近5次
            </a>
          </Popover>
        </Form.Item>
        <div style={{ fontSize: 12 }}>
          <div style={{ marginBottom: 12 }}>
            {commonCronExpressions.map((item, index) => (
              <Button
                key={index}
                size="small"
                type="link"
                style={{
                  fontSize: 12,
                  padding: '0 8px',
                  height: 'auto',
                  border: '1px solid #d9d9d9',
                  marginRight: 8,
                  marginBottom: 4,
                }}
                onClick={() => applyCronExpression(item.expression)}
              >
                {item.label}
              </Button>
            ))}
          </div>
        </div>
        <Divider style={{ margin: '4px 0' }} />
        <Form.Item
          name="scheduleStatus"
          label="启动调度"
          rules={[{ required: true, message: '必须选择是否调度' }]}
        >
          <Radio.Group>
            <Radio value="RUNNING">正常调度</Radio>
            <Radio value="STOPPED">暂停调度</Radio>
          </Radio.Group>
        </Form.Item>
      </Form>
    </>
  );
};
