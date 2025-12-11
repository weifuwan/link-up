import { Button } from 'antd';
import React from 'react';
import "./index.less"

interface FooterActionsProps {
  onSubmit: () => void;
  submitText?: string;
  loading?: boolean;
}

export const FooterActions: React.FC<FooterActionsProps> = ({ 
  onSubmit, 
  submitText = "新建任务" ,
  loading
}) => {
  return (
    <div className="dc-ui-pro-step-footer">
      <div className="dc-ui-pro-step-footer-main">
        <Button loading={loading} type="primary" onClick={onSubmit} size="small">
          {submitText}
        </Button>
      </div>
    </div>
  );
};