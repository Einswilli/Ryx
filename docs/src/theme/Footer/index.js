import React from 'react';
import Link from '@docusaurus/Link';
import './styles.css';

function FooterColumn({ title, items }) {
  return (
    <div className="footer-col">
      <h4 className="footer-col__title">{title}</h4>
      <ul className="footer-col__list">
        {items.map((item, i) => (
          <li key={i}>
            <Link
              className="footer-col__link"
              to={item.to}
              href={item.href}
            >
              {item.label}
            </Link>
          </li>
        ))}
      </ul>
    </div>
  );
}

export default function Footer() {
  const { footer } = {
    footer: {
      links: [
        {
          title: 'Documentation',
          items: [
            { label: 'Getting Started', to: '/getting-started/installation' },
            { label: 'Core Concepts', to: '/core-concepts/models' },
            { label: 'Querying', to: '/querying/filtering' },
            { label: 'API Reference', to: '/reference/api-reference' },
            { label: 'Internals', to: '/internals/architecture' },
          ],
        },
        {
          title: 'Community',
          items: [
            { label: 'GitHub', href: 'https://github.com/AllDotPy/Ryx' },
            { label: 'Contributing Guide', href: 'https://github.com/AllDotPy/Ryx/blob/main/CONTRIBUTING.md' },
            { label: 'Report an Issue', href: 'https://github.com/AllDotPy/Ryx/issues' },
          ],
        },
        {
          title: 'Project',
          items: [
            { label: 'AllDotPy', href: 'https://github.com/AllDotPy' },
            { label: 'AGPL-3.0 License', href: 'https://github.com/AllDotPy/Ryx/blob/main/LICENSE' },
            { label: 'MIT / Apache-2.0', href: 'https://github.com/AllDotPy/Ryx/blob/main/Cargo.toml' },
          ],
        },
      ],
    },
  };

  return (
    <footer className="ryx-footer">
      <div className="ryx-footer__container">
        <div className="ryx-footer__top">
          <div className="ryx-footer__brand">
            <div className="ryx-footer__logo">
              <svg viewBox="0 0 64 64" width="36" height="36" fill="none">
                <rect width="64" height="64" rx="12" fill="#6c5ce7" />
                <text x="32" y="42" textAnchor="middle" fontFamily="system-ui" fontSize="28" fontWeight="800" fill="white">R</text>
              </svg>
              <span className="ryx-footer__name">Ryx ORM</span>
            </div>
            <p className="ryx-footer__tagline">
              Django-style Python ORM. Powered by Rust.
            </p>
            <a
              href="https://github.com/AllDotPy/Ryx"
              target="_blank"
              rel="noopener noreferrer"
              className="ryx-footer__star"
            >
              <svg viewBox="0 0 16 16" width="16" height="16" fill="currentColor">
                <path d="M8 .25a.75.75 0 01.673.418l1.882 3.815 4.21.612a.75.75 0 01.416 1.279l-3.046 2.97.719 4.192a.75.75 0 01-1.088.791L8 12.347l-3.766 1.98a.75.75 0 01-1.088-.79l.72-4.194L.818 6.374a.75.75 0 01.416-1.28l4.21-.611L7.327.668A.75.75 0 018 .25z" />
              </svg>
              <span>Star us on GitHub</span>
              <svg viewBox="0 0 16 16" width="14" height="14" fill="currentColor" className="ryx-footer__star-icon">
                <path d="M7.47 10.78a.75.75 0 001.06 0l3.75-3.75a.75.75 0 00-1.06-1.06L8.75 8.44V1.75a.75.75 0 00-1.5 0v6.69L4.78 5.97a.75.75 0 00-1.06 1.06l3.75 3.75zM3.75 13a.75.75 0 000 1.5h8.5a.75.75 0 000-1.5h-8.5z" />
              </svg>
            </a>
          </div>
          <div className="ryx-footer__links">
            {footer.links.map((col, i) => (
              <FooterColumn key={i} title={col.title} items={col.items} />
            ))}
          </div>
        </div>
        <div className="ryx-footer__bottom">
          <div className="ryx-footer__divider" />
          <p className="ryx-footer__copyright">
            Copyright © {new Date().getFullYear()} AllDotPy — Python: AGPL-3.0 · Rust: MIT OR Apache-2.0
          </p>
        </div>
      </div>
    </footer>
  );
}
