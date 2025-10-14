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
          lastVersion: 'current',
          versions: {
            current: {
              label: '3.1.0',
              path: '3.1.0',
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
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'Apache Ignite 3',
      logo: {
        alt: 'Apache Ignite Logo',
        src: 'img/logo.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'tutorialSidebar',
          position: 'left',
          label: 'Documentation',
        },
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
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Documentation',
          items: [
            {
              label: 'Apache Ignite 3',
              to: '/',
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
