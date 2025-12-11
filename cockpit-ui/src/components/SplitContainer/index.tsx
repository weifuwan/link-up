import { Splitter } from 'antd';
import React from 'react';

interface SplitContainerProps {
  leftContent?: React.ReactNode;
  rightContent?: React.ReactNode;
  defaultLeftSize?: string;
  minLeftSize?: string;
  maxLeftSize?: string;
  containerStyle?: React.CSSProperties;
  splitterStyle?: React.CSSProperties;
  height?: string;
}

const SplitContainer: React.FC<SplitContainerProps> = ({
  leftContent,
  rightContent,
  defaultLeftSize = '40%',
  minLeftSize = '20%',
  maxLeftSize = '70%',
  containerStyle = {},
  splitterStyle = {},
  height = "calc(100vh - 59px)"
}) => {
  const defaultContainerStyle = {
    marginTop: 0,
    ...containerStyle,
  };

  const defaultSplitterStyle = {
    height: height,
    boxShadow: '0 0 10px rgba(0, 0, 0, 0.1)',
    ...splitterStyle,
  };

  return (
    <div style={defaultContainerStyle}>
      <Splitter style={defaultSplitterStyle}>
        <Splitter.Panel defaultSize={defaultLeftSize} min={minLeftSize} max={maxLeftSize}>
          {leftContent}
        </Splitter.Panel>
        <Splitter.Panel>{rightContent}</Splitter.Panel>
      </Splitter>
    </div>
  );
};

export default SplitContainer;
