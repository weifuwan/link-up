export enum ConfigOperate {
  Add,
  Edit,
}

export type ConfigProps = {
  id?: number;
  dsType?: string;
  valueName?: string;
  value?: string;
  status?: 1 | 2;
  url?: string;
  customParams?: string;
  dsName?: string;
};

export type AddConfigProps = Omit<ConfigProps, 'id' | 'operator'>;
export type EditConfigProps = Omit<ConfigProps, 'operator'>;


export const sourceList = [
  {
    groupName: "常用关系型数据库",
    datasourceList: [

      {
        onlyDiScript: false,
        text: "MYSQL",
        type: "MYSQL",
      },
      {
        onlyDiScript: false,
        text: "ORACLE",
        type: "ORACLE",
      },
      {
        onlyDiScript: false,
        text: "POSTGRESQL",
        type: "POSTGRESQL",
      },
      {
        onlyDiScript: false,
        text: "SQLSERVER",
        type: "SQLSERVER",
      },
      // {
      //   onlyDiScript: false,
      //   text: "SQLITE",
      //   type: "SQLITE",
      // },
      {
        onlyDiScript: false,
        text: "MONGODB",
        type: "MONGODB",
      },
      {
        onlyDiScript: false,
        text: "DB2",
        type: "DB2",
      },
      {
        onlyDiScript: false,
        text: "CACHE",
        type: "CACHE",
      },

    ],
  },
  {
    groupName: "国产关系型数据库",
    datasourceList: [
      {
        onlyDiScript: false,
        text: "OPENGAUSS",
        type: "OPENGAUSS",
      },
      {
        onlyDiScript: false,
        text: "DAMENG",
        type: "DAMENG",
      }, 
      // {
      //   onlyDiScript: false,
      //   text: "KINGBASE",
      //   type: "KINGBASE",
      // },
    ],
  },
  {
    groupName: "NoSQL",
    datasourceList: [
      // {
      //   onlyDiScript: false,
      //   text: "ELASTICSEARCH",
      //   type: "ELASTICSEARCH",
      // },

    ],
  },
  {
    groupName: "大数据存储",
    datasourceList: [
      {
        onlyDiScript: false,
        text: "DORIS",
        type: "DORIS",
      },
      {
        onlyDiScript: false,
        text: "HIVE3",
        type: "HIVE3",
      },
      {
        onlyDiScript: false,
        text: "STARROCKS",
        type: "STARROCKS",
      },
      {
        onlyDiScript: false,
        text: "CLICKHOUSE",
        type: "CLICKHOUSE",
      },
    ],
  },
];