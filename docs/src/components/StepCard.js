import React from 'react';
import './StepCard.css';

export function StepCard({ number, title, children }) {
  return (
    <div className="step-card">
      <div className="step-card__header">
        <div className="step-card__number">{number}</div>
        <h3 className="step-card__title">{title}</h3>
      </div>
      <div className="step-card__body">{children}</div>
    </div>
  );
}
