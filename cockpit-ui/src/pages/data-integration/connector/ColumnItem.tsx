import { Col, Row } from 'antd';
import React from 'react';
import './index.less';

// 类型定义
interface DataItem {
  value?: string;
  sourceType?: string;
}

interface AppProps {
  readerItems: DataItem[];
  writerItems: DataItem[];
}

const ColumnItem: React.FC<AppProps> = ({ readerItems, writerItems }) => {
  // 渲染数据项
  const renderDataItem = (item: DataItem, isWriter = false, padding = 6.1) => (
    <div
      className="dc-column-header-real"
      style={{
        background: '#f2f4f7',
        fontWeight: 500,
        padding: padding + 'px 0',
        border: '1px solid rgba(0, 0, 0, 0.1)',
        // 移除相邻边框的重合
        borderRight: 'none',
        borderBottom: 'none',
        borderLeft: 'none',
        borderTop: 'none',
      }}
    >
      <Row justify={isWriter ? 'space-between' : 'center'}>
        <Col span={12} style={{ textAlign: 'center', fontSize: 12 }}>
          {item?.value || ''}
        </Col>
        <Col span={12} style={{ textAlign: 'center', fontSize: 12 }}>
          {item?.sourceType || ''}
        </Col>
      </Row>
    </div>
  );

  // 渲染标题 Header
  const renderHeader = () => (
    <div
      style={{
        background: '#f2f4f7',
        fontWeight: 500,
        padding: '8px 0',
        // 移除相邻边框的重合
        borderRight: 'none',
        borderBottom: '1px solid rgba(0, 0, 0, 0.1)',
        borderTop: 'none',
        borderLeft: 'none',
      }}
    >
      <Row justify="space-around">
        <Col span={12} style={{ textAlign: 'center', fontSize: 12 }}>
          字段名称
        </Col>
        <Col span={12} style={{ textAlign: 'center', fontSize: 12 }}>
          字段类型
        </Col>
      </Row>
    </div>
  );

  // 连接线组件
  const ConnectionComponent = ({ readerItem, index, writerItems }) => {
    // 空指针判断
    if (!readerItem || !writerItems || !Array.isArray(writerItems)) {
      return <div style={{ height: '25px' }}></div>;
    }

    // 按照 index 去 writerItems 中获取对应位置的字段
    const writerItem = writerItems[index];

    // 判断是否可以连接：检查对应位置的 writerItem 是否存在且字段名匹配
    const canConnect =
      writerItem && readerItem.value && writerItem.value && readerItem.value === writerItem.value;

    // 如果不能连接，返回空
    if (!canConnect) {
      return <div style={{ height: '25px' }}></div>;
    }

    return (
      <div style={{ paddingTop: 6, paddingBottom: 5 }}>
        <div className="connection dc-ui-flow-connection">
          <div className="canvas-wrap">
            <div className="canvas-content flow-connection-content-1725687900057-0">
              <div
                style={{
                  position: 'relative',
                  width: 'auto',
                  height: 'auto',
                  padding: 0,
                  margin: 0,
                  borderWidth: 0,
                  cursor: 'default',
                }}
              >
                <div className="pink-container">
                  <div className="pink-dot">
                    <div className="pink-hot-inner"></div>
                  </div>
                  <div className="pink-dashed-line"></div>
                  <div className="pink-dashed-line"></div>
                  <div className="pink-di-arrow-right-triangle"></div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  };

  return (
    <>
      <Row justify="space-around">
        <Col span={20}>
          {/* 标题 Header */}
          <Row style={{ marginBottom: 0 }}>
            <Col span={9}>
              <div
                style={{
                  border: '1px solid rgba(0, 0, 0, 0.1)',
                  borderBottom: 'none',
                }}
              >
                {renderHeader()}
              </div>
            </Col>
            <Col span={6}></Col>
            <Col span={9}>
              <div
                style={{
                  border: '1px solid rgba(0, 0, 0, 0.1)',
                  borderBottom: 'none',
                }}
              >
                {renderHeader()}
              </div>
            </Col>
          </Row>

          {/* 数据行 */}
          <div
            style={{
              height: 300,
              overflowY: 'auto',
              scrollbarWidth: 'none' /* Firefox */,
              msOverflowStyle: 'none' /* IE and Edge */,
            }}
          >
            <Row>
              {/* Reader 列 */}
              <Col span={9}>
                <div
                  style={{
                    border: '1px solid rgba(0, 0, 0, 0.1)',
                    borderTop: 'none',
                  }}
                >
                  {readerItems.map((readerItem, index) => {
                    const hasReader = readerItem && (readerItem.value || readerItem.sourceType);
                    return (
                      hasReader && (
                        <div
                          key={index}
                          style={{
                            borderBottom:
                              index < readerItems.length - 1
                                ? '1px solid rgba(0, 0, 0, 0.1)'
                                : 'none',
                          }}
                        >
                          {renderDataItem(readerItem)}
                        </div>
                      )
                    );
                  })}
                </div>
              </Col>

              {/* 连接线 */}
              <Col span={6}>
                {readerItems?.map((readerItem, index) => {
                  return (
                    <ConnectionComponent
                      readerItem={readerItem}
                      index={index}
                      writerItems={writerItems}
                    />
                  );
                })}
              </Col>

              {/* Writer 列 */}
              <Col span={9}>
                <div
                  style={{
                    border: '1px solid rgba(0, 0, 0, 0.1)',
                    borderTop: 'none',
                  }}
                >
                  {writerItems.map((writerItem, index) => {
                    const hasWriter = writerItem && (writerItem.value || writerItem.sourceType);
                    return (
                      hasWriter && (
                        <div
                          key={index}
                          style={{
                            borderBottom:
                              index < writerItems.length - 1
                                ? '1px solid rgba(0, 0, 0, 0.1)'
                                : 'none',
                          }}
                        >
                          {renderDataItem(writerItem)}
                        </div>
                      )
                    );
                  })}
                </div>
              </Col>
            </Row>
          </div>
        </Col>
      </Row>
    </>
  );
};

export default ColumnItem;
