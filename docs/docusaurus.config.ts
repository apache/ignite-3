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
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: '/docs/',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'apache', // Usually your GitHub org/user name.
  projectName: 'ignite-3', // Usually your repo name.

  onBrokenLinks: 'throw',

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
          // Edit URL points to Apache Ignite 3 repository
          editUrl:
            'https://github.com/apache/ignite-3/tree/main/docs/',
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

  themeConfig: {
    image: 'img/logo.svg',
    colorMode: {
      defaultMode: 'light',
      disableSwitch: false,
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'Apache Ignite 3',
      logo: {
        alt: 'Apache Ignite Logo',
        src: 'img/logo.svg',
        href: '/3.1.0/',
        target: '_self',
      },
      items: [
        {
          type: 'docsVersionDropdown',
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
      darkTheme: prismThemes.dracula,
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
