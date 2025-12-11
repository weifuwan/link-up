import React from 'react';
import Header from '@/components/Header';

interface ConfigSectionProps {
  title: string;
  children: React.ReactNode;
  extra?: React.ReactNode;
}

export const ConfigSection: React.FC<ConfigSectionProps> = ({ 
  title, 
  children, 
  extra 
}) => {
  return (
    <div
      style={{
        padding: '12px 24px 16px',
        margin: 16,
        backgroundColor: 'white',
        paddingBottom: 24,
      }}
    >
      <Header title={<span style={{ fontSize: 14 }}>{title}</span>} />
      {extra}
      {children}
    </div>
  );
};