export default [

  {
    path: '/data-integration',
    name: '数据集成',
    icon: 'cloudSync',
    routes: [
      {
        path: '/data-integration',
        redirect: '/data-integration/data-source',
      },
      {
        icon: 'database',
        name: '数据源',
        path: '/data-integration/data-source',
        component: './data-integration/data-source',
      },
      {
        icon: 'product',
        name: '数据同步',
        path: '/data-integration/connector',
        component: './data-integration/connector',
      },
      {
        icon: 'cloud',
        name: '任务运维',
        path: '/data-integration/ops',
        component: './data-integration/ops',
      },
    ],
  },

  {
    path: '/system-monitor',
    name: '系统监控',
    icon: 'monitor',
    component: './system-monitor',
  },
  {
    path: '/',
    redirect: '/data-integration',
  },
  {
    path: '*',
    layout: false,
    component: './404',
  },
];
