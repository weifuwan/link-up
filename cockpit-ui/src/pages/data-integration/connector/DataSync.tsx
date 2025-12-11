import { SendOutlined } from '@ant-design/icons';
import { Button, Select } from 'antd';
import React, { useMemo, useState } from 'react';

// 图标组件
import MysqlIcon from '@/pages/data-integration/data-source/icon/MysqlIcon';
import OracleIcon from '@/pages/data-integration/data-source/icon/OracleIcon';
import CacheIcon from '../data-source/icon/CacheIcon';
import ClickhouseIcon from '../data-source/icon/ClickhouseIcon';
import DaMengIcon from '../data-source/icon/DamengIcon';
import DB2Icon from '../data-source/icon/DB2Icon';
import DorisIcon from '../data-source/icon/DorisIcon';
import HiveIcon from '../data-source/icon/HiveIcon';
import MongoDBIcon from '../data-source/icon/MongoDBIcon';
import OpenGaussIcon from '../data-source/icon/OpenGaussIcon';
import PostgreSQL from '../data-source/icon/PsSqlIcon';
import SQLServer from '../data-source/icon/SQLServer';
import StarRocksIcon from '../data-source/icon/StarRocksIcon';
import IconRightArrow from './IconRightArrow';
import './index.less';

const { Option } = Select;

// 类型定义
interface DataSourceType {
  value: string;
  label: React.ReactNode;
  icon?: React.ReactNode;
}

interface DataSyncHeaderProps {
  setDetail: (value: boolean) => void;
  setParams: (value: SyncParams) => void;
}

export interface SyncParams {
  sourceType: string;
  targetType: string;
}

// 生成数据源选项配置
const generateDataSourceOptions = (): DataSourceType[] => [
  {
    value: 'MYSQL',
    label: (
      <div style={{ display: 'flex', alignItems: 'center' }}>
        <MysqlIcon height="24px" width="24px" />
        <span style={{ marginLeft: 8 }}>MYSQL</span>
      </div>
    ),
  },
  {
    value: 'ORACLE',
    label: (
      <div style={{ display: 'flex', alignItems: 'center' }}>
        <OracleIcon />
        <span style={{ marginLeft: 8 }}>ORACLE</span>
      </div>
    ),
  },
  {
    value: 'POSTGRESQL',
    label: (
      <div style={{ display: 'flex', alignItems: 'center' }}>
        <PostgreSQL />
        <span style={{ marginLeft: 8 }}>POSTGRESQL</span>
      </div>
    ),
  },
  {
    value: 'SQLSERVER',
    label: (
      <div style={{ display: 'flex', alignItems: 'center' }}>
        <SQLServer />
        <span style={{ marginLeft: 8 }}>SQLSERVER</span>
      </div>
    ),
  },
  // {
  //   value: 'SQLITE',
  //   label: (
  //     <div style={{ display: 'flex', alignItems: 'center' }}>
  //       <SQLite />
  //       <span style={{ marginLeft: 8 }}>SQLITE</span>
  //     </div>
  //   ),
  // },
  {
    value: 'MONGODB',
    label: (
      <div style={{ display: 'flex', alignItems: 'center' }}>
        <MongoDBIcon />
        <span style={{ marginLeft: 8 }}>MONGODB</span>
      </div>
    ),
  },
  {
    value: 'DB2',
    label: (
      <div style={{ display: 'flex', alignItems: 'center' }}>
        <DB2Icon />
        <span style={{ marginLeft: 8 }}>DB2</span>
      </div>
    ),
  },
  {
    value: 'CACHE',
    label: (
      <div style={{ display: 'flex', alignItems: 'center' }}>
        <CacheIcon />
        <span style={{ marginLeft: 8 }}>CACHE</span>
      </div>
    ),
  },
  // {
  //   value: 'ELASTICSEARCH',
  //   label: (
  //     <div style={{ display: 'flex', alignItems: 'center' }}>
  //       <ElasticSearchIcon />
  //       <span style={{ marginLeft: 8 }}>ELASTICSEARCH</span>
  //     </div>
  //   ),
  // },
  {
    value: 'OPENGAUSS',
    label: (
      <div style={{ display: 'flex', alignItems: 'center' }}>
        <OpenGaussIcon />
        <span style={{ marginLeft: 8 }}>OPENGAUSS</span>
      </div>
    ),
  },
  {
    value: 'DORIS',
    label: (
      <div style={{ display: 'flex', alignItems: 'center' }}>
        <DorisIcon />
        <span style={{ marginLeft: 8 }}>DORIS</span>
      </div>
    ),
  },
  {
    value: 'HIVE3',
    label: (
      <div style={{ display: 'flex', alignItems: 'center' }}>
        <HiveIcon />
        <span style={{ marginLeft: 8 }}>HIVE</span>
      </div>
    ),
  },
  {
    value: 'STARROCKS',
    label: (
      <div style={{ display: 'flex', alignItems: 'center' }}>
        <StarRocksIcon />
        <span style={{ marginLeft: 8 }}>STARROCKS</span>
      </div>
    ),
  },
  {
    value: 'CLICKHOUSE',
    label: (
      <div style={{ display: 'flex', alignItems: 'center' }}>
        <ClickhouseIcon />
        <span style={{ marginLeft: 8 }}>CLICKHOUSE</span>
      </div>
    ),
  },
  {
    value: 'DAMENG',
    label: (
      <div style={{ display: 'flex', alignItems: 'center' }}>
        <DaMengIcon />
        <span style={{ marginLeft: 8 }}>DAMENG</span>
      </div>
    ),
  },
];

// 数据源选择器组件
interface DataSourceSelectProps {
  value: string;
  onChange: (value: string) => void;
  placeholder: string;
  prefix: string;
}

const DataSourceSelect: React.FC<DataSourceSelectProps> = ({
  value,
  onChange,
  placeholder,
  prefix,
}) => {
  const dataSourceOptions = useMemo(() => generateDataSourceOptions(), []);

  return (
    <Select
      showSearch
      placeholder={placeholder}
      value={value}
      optionFilterProp="label"
      onChange={onChange}
      suffixIcon={<SendOutlined />}
      style={{ width: '42%' }}
      prefix={<span style={{ fontSize: 12 }}>{prefix}</span>}
      filterOption={(input, option) => {
        const labelText = typeof option?.label === 'string' ? option.label : 'MYSQL';
        return labelText.toLowerCase().includes(input.toLowerCase());
      }}
      options={dataSourceOptions}
    />
  );
};

const DataSyncHeader: React.FC<DataSyncHeaderProps> = ({ setDetail, setParams }) => {
  const [sourceType, setSourceType] = useState<string>('MYSQL');
  const [targetType, setTargetType] = useState<string>('MYSQL');

  const handleSourceChange = (value: string) => {
    setSourceType(value);
  };

  const handleTargetChange = (value: string) => {
    setTargetType(value);
  };

  const handleCreateClick = () => {
    setDetail(true);
    setParams({
      sourceType,
      targetType,
    });
  };

  const isButtonDisabled = !sourceType || !targetType;

  return (
    <div className="jy-dc-ui-pro-header">
      <div className="jy-dc-ui-pro-header-heading">
        <div className="jy-dc-ui-pro-header-heading-title">
          <div className="jy-dc-ui-title-dc-ui-title-large-dc-ui-title-LR">
            <div className="jy-dc-ui-title-name">
              <div className="jy-dc-ui-title-name-content" title="同步任务">
                同步任务
              </div>
              <div className="jy-dc-ui-title-sub-name">
                完全向导式白屏化配置，轻松上手企业级数据同步任务配置。先选择您要同步的來源和去向类型，系统会自动展示它们支持的所有同步方案，一步即可建立所需同步任务。
              </div>
            </div>
          </div>
        </div>
      </div>

      <div className="jy-dc-ui-pro-header-footer">
        <div style={{ padding: '12px 24px 16px' }}>
          <div style={{ display: 'flex', alignItems: 'center' }}>
            <DataSourceSelect
              value={sourceType}
              onChange={handleSourceChange}
              placeholder="来源"
              prefix="来源："
            />

            <div style={{ display: 'flex', alignItems: 'center', margin: '0 8px' }}>
              <IconRightArrow />
            </div>

            <DataSourceSelect
              value={targetType}
              onChange={handleTargetChange}
              placeholder="去向"
              prefix="去向："
            />

            <Button
              style={{ marginLeft: '1.5%', width: '13%' }}
              type="primary"
              disabled={isButtonDisabled}
              onClick={handleCreateClick}
            >
              开始创建
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default DataSyncHeader;
