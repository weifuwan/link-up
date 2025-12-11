import { AvatarDropdown, AvatarName } from '@/components';
import { LinkOutlined } from '@ant-design/icons';
import type { Settings as LayoutSettings } from '@ant-design/pro-components';
import { SettingDrawer } from '@ant-design/pro-components';
import type { RunTimeLayoutConfig } from '@umijs/max';
import { history, Link } from '@umijs/max';
import { FloatButton, Popover } from 'antd';
import Draggable from 'react-draggable';
import defaultSettings from '../config/defaultSettings';
import RobotIcon from './icon/RobotIcon';

import { errorConfig } from './requestErrorConfig';
import './tailwind.css';
const isDev = process.env.NODE_ENV === 'development';
// const loginPath = '/user/login';
const loginPath = '/user/login';

const DraggableHelpButton = () => {
  return (
    <Draggable bounds="body" handle=".ant-float-btn-body">
      <div
        style={{
          position: 'fixed',
          zIndex: 100,
          right: 24,
          bottom: 24,
          cursor: 'move',
        }}
      >
        <Popover
          content={
            <>
              <div style={{ height: '70vh', width: '40vh', minHeight: 300 }}>
                
              </div>
            </>
          }
          title={<div style={{fontSize: 16}}>佳缘机器人</div>}
          trigger="click"
          placement="topLeft"
        >
          <FloatButton
            type="primary"
            shape="circle"
            icon={<RobotIcon />}
            // icon={<RobotFilled />}
            // tooltip="帮助中心"
            style={{ height: 56, width: 58 }}
            className="ant-float-btn-body"
          />
        </Popover>
        {/* <RobotIcon /> */}
      </div>
    </Draggable>
  );
};

/**
 * @see  https://umijs.org/zh-CN/plugins/plugin-initial-state
 * */
export async function getInitialState(): Promise<{
  settings?: Partial<LayoutSettings>;
  currentUser?: API.CurrentUser;
  loading?: boolean;
  fetchUserInfo?: () => Promise<API.CurrentUser | undefined>;
}> {
  const fetchUserInfo = async () => {
    try {
      // const resp = await getUserInfo();
      // HttpUtils.get()

      // const userInfo: any = localStorage.getItem("userInfo");

      // const resp = JSON.parse(userInfo) || {};

      // console.log(resp);
      // if (resp === undefined || resp?.code !== 0 || resp === null) {
      //   console.log("-------------");
      //   history.push(loginPath);
      // } else {
      //   return {
      //     userName: resp?.data.realName,
      //     name: resp?.data.realName,
      //     ip: resp?.data.ip,
      //   } as API.CurrentUser;
      // }
      const { location } = history;
      history.push(location.pathname);
    } catch (error) {
      history.push(loginPath);
    }
    return undefined;
  };
  // 如果不是登录页面，执行
  const { location } = history;
  if (location.pathname !== loginPath) {
    const currentUser = await fetchUserInfo();
    return {
      fetchUserInfo,
      currentUser,
      settings: defaultSettings as Partial<LayoutSettings>,
    };
  }
  return {
    fetchUserInfo,
    settings: defaultSettings as Partial<LayoutSettings>,
  };
}

// ProLayout 支持的api https://procomponents.ant.design/components/layout
export const layout: RunTimeLayoutConfig = ({ initialState, setInitialState }) => {
  return {
    actionsRender: () => [
      // <Question key="doc" />, <SelectLang key="SelectLang" />
    ],
    avatarProps: {
      src: initialState?.currentUser?.avatar,
      title: <AvatarName />,
      render: (_, avatarChildren) => {
        return <AvatarDropdown>{avatarChildren}</AvatarDropdown>;
      },
    },
    waterMarkProps: {
      content: initialState?.currentUser?.name,
    },
    footerRender: () => <> </>,
    // onPageChange: () => {
    //   const { location } = history;
    //   // 如果没有登录，重定向到 login
    //   if (!initialState?.currentUser && location.pathname !== loginPath) {
    //     history.push(loginPath);
    //   }
    // },
    onPageChange: () => {
      const { location } = history;
      // 如果没有登录，重定向到 login
      if (!initialState?.currentUser && location.pathname !== loginPath) {
        // history.push(loginPath);
      }
    },
    bgLayoutImgList: [
      {
        src: 'https://mdn.alipayobjects.com/yuyan_qk0oxh/afts/img/D2LWSqNny4sAAAAAAAAAAAAAFl94AQBr',
        left: 85,
        bottom: 100,
        height: '303px',
      },
      {
        src: 'https://mdn.alipayobjects.com/yuyan_qk0oxh/afts/img/C2TWRpJpiC0AAAAAAAAAAAAAFl94AQBr',
        bottom: -68,
        right: -45,
        height: '303px',
      },
      {
        src: 'https://mdn.alipayobjects.com/yuyan_qk0oxh/afts/img/F6vSTbj8KpYAAAAAAAAAAAAAFl94AQBr',
        bottom: 0,
        left: 0,
        width: '331px',
      },
    ],
    links: isDev
      ? [
          <Link key="openapi" to="/umi/plugin/openapi" target="_blank">
            <LinkOutlined />
            <span>OpenAPI 文档</span>
          </Link>,
        ]
      : [],
    menuHeaderRender: undefined,
    // 自定义 403 页面
    // unAccessible: <div>unAccessible</div>,
    // 增加一个 loading 的状态
    childrenRender: (children) => {
      // if (initialState?.loading) return <PageLoading />;
      return (
        <>
          {children}
          {/* <DraggableHelpButton /> */}
          {isDev && (
            <SettingDrawer
              disableUrlParams
              enableDarkTheme
              settings={initialState?.settings}
              onSettingChange={(settings) => {
                setInitialState((preInitialState) => ({
                  ...preInitialState,
                  settings,
                }));
              }}
            />
          )}
        </>
      );
    },
    ...initialState?.settings,
  };
};

/**
 * @name request 配置，可以配置错误处理
 * 它基于 axios 和 ahooks 的 useRequest 提供了一套统一的网络请求和错误处理方案。
 * @doc https://umijs.org/docs/max/request#配置
 */
export const request = {
  ...errorConfig,
};
