import type { SidebarsConfig } from "@docusaurus/plugin-content-docs";

/**
 * Apache Ignite 3 Documentation Sidebar Configuration
 *
 * Complete navigation structure with all converted pages.
 * Document IDs match the frontmatter 'id' field in each file.
 */
const sidebars: SidebarsConfig = {
  tutorialSidebar: [
    {
      type: "doc",
      id: "index",
      label: "Apache Ignite 3 Documentation",
    },

    // Getting Started (8 pages)
    {
      type: "category",
      label: "Getting Started",
      link: { type: "doc", id: "getting-started/index" },
      items: [
        "getting-started/intro",
        "getting-started/quick-start",
        "getting-started/start-cluster",
        "getting-started/work-with-sql",
        "getting-started/key-value-api",
        "getting-started/embedded-mode",
        "getting-started/best-practices",
        "getting-started/migrate-from-ignite-2",
      ],
    },

    // Develop (28 pages)
    {
      type: "category",
      label: "Develop and Build",
      link: { type: "doc", id: "develop/index" },
      items: [
        {
          type: "category",
          label: "Ignite Clients",
          link: { type: "doc", id: "develop/ignite-clients/index" },
          items: [
            "develop/ignite-clients/java-client",
            "develop/ignite-clients/dotnet-client",
            "develop/ignite-clients/cpp-client",
          ],
        },
        {
          type: "category",
          label: "Connect to Ignite",
          link: { type: "doc", id: "develop/connect-to-ignite/index" },
          items: [
            "develop/connect-to-ignite/jdbc",
            {
              type: "category",
              label: "ODBC Driver",
              link: { type: "doc", id: "develop/connect-to-ignite/odbc" },
              items: [
                "develop/connect-to-ignite/odbc-connection-string",
                "develop/connect-to-ignite/odbc-querying-data",
              ],
            },
            "develop/connect-to-ignite/python",
          ],
        },
        {
          type: "category",
          label: "Work with Data",
          link: { type: "doc", id: "develop/work-with-data/index" },
          items: [
            "develop/work-with-data/table-api",
            "develop/work-with-data/transactions",
            "develop/work-with-data/streaming",
            "develop/work-with-data/compute",
            "develop/work-with-data/serialization",
            "develop/work-with-data/code-deployment",
            "develop/work-with-data/events",
            "develop/work-with-data/events-list",
            "develop/work-with-data/java-to-tables",
            "develop/work-with-data/java-client-logging",
          ],
        },
        {
          type: "category",
          label: "Integrate",
          link: { type: "doc", id: "develop/integrate/index" },
          items: [
            "develop/integrate/spring-boot",
            "develop/integrate/spring-data",
          ],
        },
      ],
    },

    // SQL (15 pages)
    {
      type: "category",
      label: "Work with SQL",
      link: { type: "doc", id: "sql/index" },
      items: [
        {
          type: "category",
          label: "SQL Fundamentals",
          link: { type: "doc", id: "sql/fundamentals/index" },
          items: ["sql/fundamentals/engine-architecture"],
        },
        {
          type: "category",
          label: "SQL Operations",
          link: { type: "doc", id: "sql/working-with-sql/index" },
          items: [
            "sql/working-with-sql/execute-queries",
            "sql/working-with-sql/system-views",
          ],
        },
        {
          type: "category",
          label: "SQL Reference",
          link: { type: "doc", id: "sql/reference/index" },
          items: [
            {
              type: "category",
              label: "Language Definition",
              link: { type: "doc", id: "sql/reference/language-definition/index" },
              items: [
                "sql/reference/language-definition/ddl",
                "sql/reference/language-definition/dml",
                "sql/reference/language-definition/distribution-zones",
                "sql/reference/language-definition/transactions",
                "sql/reference/language-definition/grammar-reference",
              ],
            },
            {
              type: "category",
              label: "Data Types and Functions",
              link: { type: "doc", id: "sql/reference/data-types-and-functions/index" },
              items: [
                "sql/reference/data-types-and-functions/data-types",
                "sql/reference/data-types-and-functions/operators-and-functions",
                "sql/reference/data-types-and-functions/operational-commands",
              ],
            },
            {
              type: "category",
              label: "SQL Conformance",
              link: { type: "doc", id: "sql/reference/sql-conformance/index" },
              items: [
                "sql/reference/sql-conformance/overview",
                "sql/reference/sql-conformance/keywords",
              ],
            },
          ],
        },
        {
          type: "category",
          label: "Advanced SQL",
          link: { type: "doc", id: "sql/advanced/index" },
          items: [
            "sql/advanced/explain-statement",
            "sql/advanced/performance-tuning",
          ],
        },
      ],
    },

    // Configure and Operate (27 pages)
    {
      type: "category",
      label: "Configure and Operate",
      link: { type: "doc", id: "configure-and-operate/index" },
      items: [
        {
          type: "category",
          label: "Installation",
          link: { type: "doc", id: "configure-and-operate/installation/index" },
          items: [
            "configure-and-operate/installation/install-zip",
            "configure-and-operate/installation/install-deb-rpm",
            "configure-and-operate/installation/install-docker",
            "configure-and-operate/installation/install-kubernetes",
          ],
        },
        {
          type: "category",
          label: "Configuration",
          link: { type: "doc", id: "configure-and-operate/configuration/index" },
          items: [
            "configure-and-operate/configuration/config-cluster-and-nodes",
            {
              type: "category",
              label: "Storage Configuration",
              link: {
                type: "doc",
                id: "configure-and-operate/configuration/config-storage-overview",
              },
              items: [
                "configure-and-operate/configuration/config-storage",
                "configure-and-operate/configuration/config-storage-volatile",
                "configure-and-operate/configuration/config-storage-persistent",
                "configure-and-operate/configuration/config-storage-rocksdb",
              ],
            },
            "configure-and-operate/configuration/config-authentication",
            "configure-and-operate/configuration/config-ssl-tls",
            "configure-and-operate/configuration/metrics-configuration",
            "configure-and-operate/configuration/config-cluster-security",
          ],
        },
        {
          type: "category",
          label: "Operations",
          link: { type: "doc", id: "configure-and-operate/operations/index" },
          items: [
            "configure-and-operate/operations/lifecycle",
            {
              type: "category",
              label: "Disaster Recovery",
              link: {
                type: "doc",
                id: "configure-and-operate/operations/disaster-recovery",
              },
              items: [
                "configure-and-operate/operations/disaster-recovery-partitions",
                "configure-and-operate/operations/disaster-recovery-system-groups",
              ],
            },
            "configure-and-operate/operations/handle-exceptions",
            "configure-and-operate/operations/colocation",
          ],
        },
        {
          type: "category",
          label: "Monitoring",
          link: { type: "doc", id: "configure-and-operate/monitoring/index" },
          items: [
            {
              type: "category",
              label: "Metrics",
              link: {
                type: "doc",
                id: "configure-and-operate/monitoring/metrics",
              },
              items: [
                "configure-and-operate/monitoring/config-metrics",
                "configure-and-operate/monitoring/available-metrics",
                "configure-and-operate/monitoring/metrics-system-views",
              ],
            },
          ],
        },
        {
          type: "category",
          label: "Configuration Reference",
          link: { type: "doc", id: "configure-and-operate/reference/index" },
          items: [
            "configure-and-operate/reference/node-configuration",
            "configure-and-operate/reference/cluster-configuration",
            "configure-and-operate/reference/cli-configuration",
            "configure-and-operate/reference/storage-profiles",
          ],
        },
      ],
    },

    // Understand (15 pages)
    {
      type: "category",
      label: "Concepts and Architecture",
      link: { type: "doc", id: "understand/index" },
      items: [
        {
          type: "category",
          label: "Core Concepts",
          link: { type: "doc", id: "understand/core-concepts/index" },
          items: [
            "understand/core-concepts/what-is-ignite",
            "understand/core-concepts/tables-and-schemas",
            "understand/core-concepts/transactions-and-mvcc",
            "understand/core-concepts/distribution-and-colocation",
            "understand/core-concepts/compute-and-events",
          ],
        },
        {
          type: "category",
          label: "Architecture",
          link: { type: "doc", id: "understand/architecture/index" },
          items: [
            "understand/architecture/storage-architecture",
            {
              type: "category",
              label: "Storage Engines",
              link: { type: "doc", id: "understand/architecture/storage-engines/index" },
              items: [
                "understand/architecture/storage-engines/aimem",
                "understand/architecture/storage-engines/aipersist",
                "understand/architecture/storage-engines/rocksdb",
              ],
            },
            "understand/architecture/data-partitioning",
          ],
        },
        {
          type: "category",
          label: "Performance",
          link: { type: "doc", id: "understand/performance/index" },
          items: [
            "understand/performance/using-explain",
            "understand/performance/explain-operators",
          ],
        },
      ],
    },

    // API Reference (33 pages)
    {
      type: "category",
      label: "Client and API Reference",
      link: { type: "doc", id: "api-reference/index" },
      items: [
        {
          type: "category",
          label: "Native Client APIs",
          link: { type: "doc", id: "api-reference/native-clients/index" },
          items: [
            {
              type: "category",
              label: "Java API (PRIMARY)",
              link: {
                type: "doc",
                id: "api-reference/native-clients/java/java-index",
              },
              items: [
                "api-reference/native-clients/java/client-api",
                "api-reference/native-clients/java/server-api",
                "api-reference/native-clients/java/tables-api",
                "api-reference/native-clients/java/data-streamer-api",
                "api-reference/native-clients/java/sql-api",
                "api-reference/native-clients/java/transactions-api",
                "api-reference/native-clients/java/compute-api",
                "api-reference/native-clients/java/catalog-api",
                "api-reference/native-clients/java/criteria-api",
                "api-reference/native-clients/java/network-api",
                "api-reference/native-clients/java/security-api",
              ],
            },
            {
              type: "category",
              label: ".NET API",
              link: {
                type: "doc",
                id: "api-reference/native-clients/dotnet/dotnet-index",
              },
              items: [
                "api-reference/native-clients/dotnet/client-api",
                "api-reference/native-clients/dotnet/tables-api",
                "api-reference/native-clients/dotnet/linq-api",
                "api-reference/native-clients/dotnet/data-streamer-api",
                "api-reference/native-clients/dotnet/sql-api",
                "api-reference/native-clients/dotnet/ado-net-api",
                "api-reference/native-clients/dotnet/transactions-api",
                "api-reference/native-clients/dotnet/compute-api",
                "api-reference/native-clients/dotnet/network-api",
              ],
            },
            {
              type: "category",
              label: "C++ API",
              link: {
                type: "doc",
                id: "api-reference/native-clients/cpp/cpp-index",
              },
              items: [
                "api-reference/native-clients/cpp/client-api",
                "api-reference/native-clients/cpp/tables-api",
                "api-reference/native-clients/cpp/sql-api",
                "api-reference/native-clients/cpp/transactions-api",
                "api-reference/native-clients/cpp/compute-api",
                "api-reference/native-clients/cpp/network-api",
              ],
            },
          ],
        },
        {
          type: "category",
          label: "SQL-Only APIs",
          link: { type: "doc", id: "api-reference/sql-only-apis/index" },
          items: [
            "api-reference/sql-only-apis/jdbc",
            "api-reference/sql-only-apis/odbc",
            "api-reference/sql-only-apis/python",
          ],
        },
        {
          type: "category",
          label: "API Documentation",
          link: { type: "doc", id: "api-reference/api/index" },
          items: [
            "api-reference/api/java-api-reference",
            "api-reference/api/dotnet-api-reference",
            "api-reference/api/cpp-api-reference",
          ],
        },
      ],
    },

    // Tools (3 pages)
    {
      type: "category",
      label: "Tools",
      link: { type: "doc", id: "tools/index" },
      items: ["tools/cli-commands", "tools/rest-api", "tools/glossary"],
    },
  ],
};

export default sidebars;
