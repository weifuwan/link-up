import './index.less';

const Header: React.FC<{ title: any }> = ({ title }) => {
  return (
    <div className="data-indicator-card-wrapper">
      <div className="data-indicator-card-title">{title}</div>
    </div>
  );
};

export default Header;
