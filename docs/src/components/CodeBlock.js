import React from 'react';
import './CodeBlock.css';

export function CodeBlock({ children, title, language = 'python' }) {
  return (
    <div className="code-block-wrapper" data-component="code-block">
      {title && <div className="code-block-title">{title}</div>}
      {children}
    </div>
  );
}
