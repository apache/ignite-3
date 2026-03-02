import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

const config: Config = {
  title: 'Apache Ignite 3 Documentation',
  tagline: 'Distributed Database for High-Performance Computing',
  favicon: 'img/favicon.ico',

  // Future flags, see https://docusaurus.io/docs/api/docusaurus-config#future
  future: {
    v4: true, // Improve compatibility with the upcoming Docusaurus v4
  },

  // Set the production url of your site here
  url: 'https://ignite.apache.org',
  // Set the /<baseUrl>/ pathname under which your site is served
  // Serves Ignite 3 docs at /docs/ignite3/ on the main website
  // Can be overridden with BASE_URL environment variable for staging builds
  baseUrl: process.env.BASE_URL || '/docs/ignite3/',

  // Enforce consistent trailing slash
  trailingSlash: false,

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'apache', // Usually your GitHub org/user name.
  projectName: 'ignite-3', // Usually your repo name.

  // Allow broken links during development while we convert content
  // Change back to 'throw' once all content is migrated
  onBrokenLinks: 'warn',

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          // Edit link disabled - docs not yet in upstream repository
          editUrl: undefined,
          routeBasePath: '/', // Serve docs at the site root
          // Versioning configuration
          includeCurrentVersion: true, // Include the current (unversioned) docs
          lastVersion: 'current', // The most recent version
          onlyIncludeVersions: ['current', '3.0.0'], // Limit which versions are included
          versions: {
            current: {
              label: '3.1.0 (Latest)',
              path: '3.1.0',
              banner: 'none', // No banner for latest version
            },
            '3.0.0': {
              label: '3.0.0',
              path: '3.0.0',
              banner: 'unmaintained', // Show banner for older version
              badge: true, // Show version badge
            },
          },
        },
        blog: false, // Disable blog functionality
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  markdown: {
    mermaid: true,
  },

  themes: [
    '@docusaurus/theme-mermaid',
    [
      require.resolve('@easyops-cn/docusaurus-search-local'),
      {
        // Indexing options
        hashed: true, // Hash the search index file for better caching
        indexDocs: true, // Index documentation pages
        indexBlog: false, // Don't index blog (disabled)
        indexPages: false, // Don't index static pages (docs-only site)
        docsRouteBasePath: '/', // Docs are served at root

        // Search UI options
        searchResultLimits: 10, // Maximum number of search results
        searchResultContextMaxLength: 100, // Maximum length of search result context
        highlightSearchTermsOnTargetPage: true, // Highlight search terms on result pages
        searchBarPosition: 'right', // Position in navbar (before version selector)

        // Language support
        language: ['en'], // English only

        // Performance options
        searchBarShortcut: true, // Enable keyboard shortcut (Ctrl+K / Cmd+K)
        searchBarShortcutHint: true, // Show keyboard shortcut hint
      },
    ],
  ],

  themeConfig: {
    image: 'img/logo.svg',
    colorMode: {
      defaultMode: 'light',
      disableSwitch: false,
      respectPrefersColorScheme: true,
    },
    navbar: {
      logo: {
        alt: 'Apache Ignite Logo',
        src: 'img/logo.svg',
        srcDark: 'img/logo-dark.svg',
        href: '/',
        target: '_self',
      },
      items: [
        {
          type: 'docsVersionDropdown',
          position: 'right',
        },
        {
          type: 'search',
          position: 'right',
        },
        {
          href: 'https://github.com/apache/ignite-3',
          label: 'GitHub',
          position: 'right',
        },
      ],
      style: 'primary',
      hideOnScroll: false,
    },
    docs: {
      sidebar: {
        hideable: true,
        autoCollapseCategories: false,
      },
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Documentation',
          items: [
            {
              label: 'Apache Ignite 3',
              to: '/3.1.0/',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'Apache Ignite',
              href: 'https://ignite.apache.org',
            },
            {
              label: 'User Mailing List',
              href: 'https://ignite.apache.org/community.html',
            },
          ],
        },
        {
          title: 'Apache Software Foundation',
          items: [
            {
              label: 'Apache Software Foundation',
              href: 'https://www.apache.org/',
            },
            {
              label: 'License',
              href: 'https://www.apache.org/licenses/',
            },
            {
              label: 'GitHub',
              href: 'https://github.com/apache/ignite-3',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} The Apache Software Foundation. Licensed under the Apache License, Version 2.0.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.oneDark, // Similar to Tokyo Night color scheme
      additionalLanguages: [
        'java',
        'csharp',
        'cpp',
        'python',
        'sql',
        'bash',
        'json',
        'yaml',
      ],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
