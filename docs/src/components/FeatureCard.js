import React from 'react';
import './FeatureCard.css';

export function FeatureCard({ icon, title, description, href }) {
  return (
    <a href={href} className="feature-card" data-component="feature-card">
      <div className="feature-card__icon">{icon}</div>
      <h3 className="feature-card__title">{title}</h3>
      <p className="feature-card__desc">{description}</p>
    </a>
  );
}

export function FeatureGrid({ children }) {
  return <div className="feature-grid">{children}</div>;
}
