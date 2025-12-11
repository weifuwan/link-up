import { DeleteOutlined } from '@ant-design/icons';
import { Popconfirm } from 'antd';
import { Tooltip } from 'knowdesign';
import styles from './index.less';

function TreeTitle(props) {
  const {
    node: { key, children, title, sourceTitle },
    refresh = () => {},
  } = props;
  //   const handleDelete = () => {
  //     deleteDataMart([key]).then(refresh);
  //   };
  console.log(sourceTitle);
  return (
    <span key={key} className={styles.treeTitle}>
      <div>
        <Tooltip style={{ maxWidth: 200 }} title={sourceTitle}>
          {sourceTitle}
        </Tooltip>
      </div>
      <div className={styles.opt} onClick={(e) => e.stopPropagation()}>
        {children.length === 0 ? (
          <Popconfirm
            title="确认要删除么？"
            // onConfirm={() => {
            //   handleDelete();
            // }}
          >
            {/* @ts-ignore */}
            <DeleteOutlined />
          </Popconfirm>
        ) : null}
      </div>
    </span>
  );
}

export default TreeTitle;
