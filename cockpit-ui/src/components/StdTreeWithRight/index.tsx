import { DatabaseOutlined, FolderOutlined, TableOutlined } from '@ant-design/icons';
import type { TreeDataNode } from 'antd';
import { Input, Space, Tree } from 'antd';
import { DirectoryTreeProps } from 'antd/es/tree';
import React, { useMemo, useState } from 'react';
import './index.less';

const { DirectoryTree } = Tree;

interface AppProps {
  directoryTreeData: TreeDataNode[];
  onSelectNode: (key: React.Key, info: any) => void;
  rightDropdown?: React.ReactNode;
  onRightClick?: (info: any) => void;
}

const getParentKey = (key: React.Key, tree: TreeDataNode[]): React.Key => {
  for (const node of tree) {
    if (node.children) {
      if (node.children.some((item) => item.id === key)) {
        return node.id;
      }
      const parentKey = getParentKey(key, node.children);
      if (parentKey) return parentKey;
    }
  }
  return '';
};

const App: React.FC<AppProps> = ({
  directoryTreeData,
  onSelectNode,
  rightDropdown,
  onRightClick,
}) => {
  const [expandedKeys, setExpandedKeys] = useState<React.Key[]>([]);
  const [searchValue, setSearchValue] = useState('');
  const [autoExpandParent, setAutoExpandParent] = useState(true);

  // Generate flattened data list for search
  const dataList = useMemo(() => {
    const list: { key: React.Key; name: string }[] = [];
    const generateList = (data: TreeDataNode[]) => {
      console.log(data);
      for (const node of data) {
        list.push({ key: node.id, name: node.name as string, type: node?.type, leaf: node?.leaf });
        if (node.children) {
          generateList(node.children);
        }
      }
    };
    generateList(directoryTreeData);
    return list;
  }, [directoryTreeData]);

  const onExpand = (newExpandedKeys: React.Key[]) => {
    setExpandedKeys(newExpandedKeys);
    setAutoExpandParent(false);
  };

  const handleSearch = (e: React.ChangeEvent<HTMLInputElement>) => {
    const { value } = e.target;
    setSearchValue(value);

    if (!value) {
      setExpandedKeys([]);
      return;
    }

    const newExpandedKeys = dataList
      .filter((item) => item.name.includes(value))
      .map((item) => getParentKey(item.key, directoryTreeData))
      .filter((item, i, self) => item && self.indexOf(item) === i);

    setExpandedKeys(newExpandedKeys);
    setAutoExpandParent(true);
  };

  const treeData = useMemo(() => {
    const getIconByType = (type: string) => {
      switch (type) {
        case 'FOLDER':
          return <FolderOutlined style={{ fontSize: '120%', marginTop: 8 }} />;
        case 'STANDARD_SET':
          return <DatabaseOutlined style={{ fontSize: '120%', marginTop: 8 }} />;
        case 'STANDARD':
          return (
            <TableOutlined
              style={{ fontSize: '120%', marginTop: 8, color: 'rgb(255, 164, 15)' }}
            />
          );
        case 'CODE':
          return (
            <TableOutlined
              style={{ fontSize: '120%', marginTop: 8, color: 'rgb(255, 164, 15)' }}
            />
          );

        case 'UNIT':
          return (
            <TableOutlined
              style={{ fontSize: '120%', marginTop: 8, color: 'rgb(255, 164, 15)' }}
            />
          );
        default:
          return <FolderOutlined style={{ fontSize: '120%', marginTop: 8 }} />;
      }
    };

    const loop = (data: TreeDataNode[]): TreeDataNode[] =>
      data.map((item) => {
        const strTitle = item.name as string;
        const index = strTitle.indexOf(searchValue);

        const title =
          index > -1 ? (
            <span key={item.id}>
              {strTitle.substring(0, index)}
              <span className="dc-site-tree-search-value">{searchValue}</span>
              {strTitle.slice(index + searchValue.length)}
            </span>
          ) : (
            <span key={item.key}>{strTitle}</span>
          );

        return {
          ...item,
          title,
          key: item.id,
          icon: getIconByType(item.type),
          isLeaf: item?.leaf,
          children: item.children ? loop(item.children) : undefined,
        };
      });

    return loop(directoryTreeData);
  }, [searchValue, directoryTreeData]);

  const handleSelect: DirectoryTreeProps['onSelect'] = (keys, info) => {
    onSelectNode(keys[0] || -1, info);
  };

  return (
    <div className="directory-tree-container">
      <Space.Compact style={{ width: '100%', marginBottom: 8 }}>
        <Input
          placeholder="搜索"
          size="small"
          onChange={handleSearch}
          allowClear
          style={{ margin: '0 12px 0px 6px' }}
        />
      </Space.Compact>

      <div>
        <DirectoryTree
          onExpand={onExpand}
          style={{ margin: '0 12px 0px 6px' }}
          expandedKeys={expandedKeys}
          autoExpandParent={autoExpandParent}
          treeData={treeData}
          defaultExpandAll
          onRightClick={onRightClick}
          onSelect={handleSelect}
        />
      </div>

      {rightDropdown}
    </div>
  );
};

export default React.memo(App);
