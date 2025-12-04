/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.cli.commands;

import static java.util.stream.Collectors.toSet;

import java.util.Set;
import java.util.stream.Stream;
import org.apache.ignite.internal.cli.config.CliConfigKeys;

/**
 * Constants to use in {@code Option} annotations for commands.
 */
public enum Options {
    CLUSTER_URL(Constants.CLUSTER_URL_OPTION, Constants.CLUSTER_URL_OPTION_DESC),
    NODE_URL(Constants.NODE_URL_OPTION, Constants.NODE_URL_OPTION_DESC),

    CLUSTER_NAME(Constants.CLUSTER_NAME_OPTION, Constants.CLUSTER_NAME_OPTION_DESC),
    NODE_NAME(Constants.NODE_NAME_OPTION, Constants.NODE_NAME_OPTION_SHORT, Constants.NODE_NAME_OPTION_DESC),

    CMG_NODE_NAME(Constants.CMG_NODE_NAME_OPTION, Constants.CMG_NODE_NAME_OPTION_DESC),
    META_STORAGE_NODE_NAME(
            Constants.META_STORAGE_NODE_NAME_OPTION,
            Constants.META_STORAGE_NODE_NAME_OPTION_DESC
    ),

    PROFILE(Constants.PROFILE_OPTION, Constants.PROFILE_OPTION_DESC),
    PROFILE_COPY_FROM(
            Constants.PROFILE_COPY_FROM_OPTION,
            Constants.PROFILE_COPY_FROM_OPTION_DESC
    ),
    PROFILE_ACTIVATE(Constants.PROFILE_ACTIVATE_OPTION, Constants.PROFILE_ACTIVATE_OPTION_DESC),

    SCRIPT_FILE(Constants.SCRIPT_FILE_OPTION, Constants.SCRIPT_FILE_OPTION_DESC),
    JDBC_URL(Constants.JDBC_URL_OPTION, Constants.JDBC_URL_OPTION_DESC),

    UNIT_PATH(Constants.UNIT_PATH_OPTION, Constants.UNIT_PATH_OPTION_DESC),
    UNIT_VERSION(Constants.VERSION_OPTION, Constants.UNIT_VERSION_OPTION_DESC),
    UNIT_NODES(Constants.UNIT_NODES_OPTION, Constants.UNIT_NODES_OPTION, Constants.UNIT_NODES_OPTION_DESC),

    PLAIN(Constants.PLAIN_OPTION, Constants.PLAIN_OPTION, Constants.PLAIN_OPTION_DESC),
    VERBOSE(Constants.VERBOSE_OPTION, Constants.VERBOSE_OPTION_SHORT, Constants.VERBOSE_OPTION_DESC),
    HELP(Constants.HELP_OPTION, Constants.HELP_OPTION_SHORT, Constants.HELP_OPTION_DESC),
    VERSION(Constants.VERSION_OPTION, Constants.VERSION_OPTION, Constants.VERSION_OPTION_DESC),

    CLUSTER_CONFIG(Constants.CLUSTER_CONFIG_OPTION, Constants.CLUSTER_CONFIG_OPTION_DESC),
    CLUSTER_CONFIG_FILE(
            Constants.CLUSTER_CONFIG_FILE_OPTION,
            Constants.CLUSTER_CONFIG_FILE_OPTION_DESC
    ),

    RECOVERY_NODE_NAMES(Constants.RECOVERY_NODE_NAMES_OPTION, Constants.RECOVERY_NODE_NAMES_OPTION_DESC),
    RECOVERY_CMG_NODES(Constants.RECOVERY_CMG_NODES_OPTION, Constants.RECOVERY_CMG_NODES_OPTION_DESC);

    private final String fullName;
    private final String shortName;
    private final String description;

    Options(String fullName, String shortName, String description) {
        this.fullName = fullName;
        this.shortName = shortName;
        this.description = description;
    }

    Options(String fullName, String description) {
        this.fullName = fullName;
        this.shortName = fullName;
        this.description = description;
    }

    public String fullName() {
        return fullName;
    }

    public String shortName() {
        return shortName;
    }

    public Set<String> names() {
        return Stream.of(fullName, shortName).collect(toSet());
    }

    public String description() {
        return description;
    }

    /** Constants for all options. */
    public static final class Constants {
        /** Cluster endpoint URL option long name. */
        public static final String CLUSTER_URL_OPTION = "--url";

        /** Cluster endpoint URL option description. */
        public static final String CLUSTER_URL_OPTION_DESC = "URL of cluster endpoint. It can be any node URL."
                + "If not set, then the default URL from the profile settings will be used";

        /** Cluster endpoint URL option description key. */
        public static final String CLUSTER_URL_KEY = CliConfigKeys.Constants.CLUSTER_URL;

        /** Node URL option long name. */
        public static final String NODE_URL_OPTION = "--url";

        /** Node URL option description. */
        public static final String NODE_URL_OPTION_DESC = "URL of a node that will be used as a communication endpoint. "
                + "It can be any node URL. If not set, then the default URL from the profile settings will be used";

        /** Profile name option long name. */
        public static final String PROFILE_OPTION = "--profile";

        /** Profile name option description. */
        public static final String PROFILE_OPTION_DESC = "Local CLI profile name. "
                + "Profile stores useful settings like default cluster URL, jdbc URL, etc";

        /** Node name option long name. */
        public static final String NODE_NAME_OPTION = "--node";

        /** Node name option short name. */
        public static final String NODE_NAME_OPTION_SHORT = "-n";

        /** Node name option description. */
        public static final String NODE_NAME_OPTION_DESC = "The name of the node to perform the operation on. "
                + "Node names can be seen in the output of the 'cluster topology' command";

        /** Verbose option long name. */
        public static final String VERBOSE_OPTION = "--verbose";

        /** Verbose option short name. */
        public static final String VERBOSE_OPTION_SHORT = "-v";

        /** Verbose option description. */
        public static final String VERBOSE_OPTION_DESC = "Show additional information: logs, REST calls. "
                + "This flag is useful for debugging. Specify multiple options to increase verbosity for REST calls. "
                + "Single option shows request and response, second option (-vv) shows headers, third one (-vvv) shows body";

        /** Help option long name. */
        public static final String HELP_OPTION = "--help";

        /** Help option short name. */
        public static final String HELP_OPTION_SHORT = "-h";

        /** Verbose option description. */
        public static final String HELP_OPTION_DESC = "Show help for the specified command";

        /** Profile copy from option long name. */
        public static final String PROFILE_COPY_FROM_OPTION = "--copy-from";

        /** Profile copy from option description. */
        public static final String PROFILE_COPY_FROM_OPTION_DESC = "Profile whose content will be copied to new one";

        /** Profile activate option long name. */
        public static final String PROFILE_ACTIVATE_OPTION = "--activate";

        /** Profile activate option description. */
        public static final String PROFILE_ACTIVATE_OPTION_DESC = "Activate new profile as current or not. "
                + "By activating a profile, you set profile settings for the current session";

        /** Cluster management node name option long name. */
        public static final String CMG_NODE_NAME_OPTION = "--cluster-management-group";

        /** Label for cluster-management-group parameter. --cluster-management-group=label. */
        public static final String CMG_NODE_NAME_PARAM_LABEL = "<node name>";

        /** Cluster management node name option description. */
        public static final String CMG_NODE_NAME_OPTION_DESC = "Names of nodes (use comma-separated list of node names "
                + "'--cluster-management-group node1, node2' "
                + "to specify more than one node) that will host the Cluster Management Group."
                + " If omitted, then --metastorage-group values will also supply the nodes for the Cluster Management Group.";

        /** Meta storage management node name option long name. */
        public static final String META_STORAGE_NODE_NAME_OPTION = "--metastorage-group";

        /** Label for metastorage-group parameter. --metastorage-group=label. */
        public static final String META_STORAGE_NODE_NAME_PARAM_LABEL = "<node name>";

        /** Meta storage node name option description. */
        public static final String META_STORAGE_NODE_NAME_OPTION_DESC = "Metastorage group nodes (use comma-separated list of node names "
                + "'--metastorage-group node1, node2' to specify more than one node) that will host the Meta Storage."
                + " If the --cluster-management-group option is omitted, the same nodes will also host the Cluster Management Group.";

        /** Cluster name option long name. */
        public static final String CLUSTER_NAME_OPTION = "--name";

        /** Cluster name option description. */
        public static final String CLUSTER_NAME_OPTION_DESC = "Human-readable name of the cluster";

        /** Plain option long name. */
        public static final String PLAIN_OPTION = "--plain";

        /** Plain option description. */
        public static final String PLAIN_OPTION_DESC = "Display output with plain formatting. "
                + "Might be useful if you want to pipe the output to another command";

        /** JDBC URL option long name. */
        public static final String JDBC_URL_OPTION = "--jdbc-url";

        /** JDBC URL option description. */
        public static final String JDBC_URL_OPTION_DESC = "JDBC url to ignite cluster. For example, 'jdbc:ignite:thin://127.0.0.1:10800'";

        /** JDBC URL option description key. */
        public static final String JDBC_URL_KEY = CliConfigKeys.Constants.JDBC_URL;

        /** SQL script file option long name. */
        public static final String SCRIPT_FILE_OPTION = "--file";

        /** SQL script file option description. */
        public static final String SCRIPT_FILE_OPTION_DESC = "Path to file with SQL commands to execute";

        /** Version option long name. */
        public static final String VERSION_OPTION = "--version";

        /** Version option description. */
        public static final String VERSION_OPTION_DESC = "Print version information";

        /** Unit version option description. */
        public static final String UNIT_VERSION_OPTION_DESC = "Unit version (x.y.z)";

        /** Unit path option long name. */
        public static final String UNIT_PATH_OPTION = "--path";

        /** Unit path option description. */
        public static final String UNIT_PATH_OPTION_DESC = "Path to deployment unit file or directory";

        /** Unit nodes option long name. */
        public static final String UNIT_NODES_OPTION = "--nodes";

        /** Unit nodes option description. */
        public static final String UNIT_NODES_OPTION_DESC = "Initial set of nodes where the unit will be deployed";

        public static final String CLUSTER_CONFIG_OPTION = "--config";

        public static final String CLUSTER_CONFIG_OPTION_DESC = "Cluster configuration that "
                + "will be applied during the cluster initialization";

        public static final String CLUSTER_CONFIG_FILE_OPTION = "--config-files";

        public static final String CLUSTER_CONFIG_FILE_OPTION_DESC = "Path to cluster configuration files (use comma-separated list of "
                + "paths '--config-files path1, path2' to specify more than one file)";

        public static final String CLUSTER_CONFIG_FILE_PARAM_LABEL = "<file path>";

        public static final String PASSWORD_OPTION = "--password";

        public static final String PASSWORD_OPTION_SHORT = "-p";

        public static final String PASSWORD_OPTION_DESC = "Password to connect to cluster";

        public static final String USERNAME_OPTION = "--username";

        public static final String USERNAME_OPTION_SHORT = "-u";

        public static final String USERNAME_OPTION_DESC = "Username to connect to cluster";

        public static final String RECOVERY_PARTITION_GLOBAL_OPTION = "--global";

        public static final String RECOVERY_PARTITION_GLOBAL_OPTION_DESC = "Get global partitions states";

        public static final String RECOVERY_PARTITION_LOCAL_OPTION = "--local";

        public static final String RECOVERY_PARTITION_LOCAL_OPTION_DESC = "Get local partition states";

        public static final String RECOVERY_PARTITION_IDS_OPTION = "--partitions";

        public static final String RECOVERY_PARTITION_IDS_OPTION_DESC = "IDs of partitions to get states. All partitions if not set";

        public static final String RECOVERY_ZONE_NAMES_OPTION = "--zones";

        public static final String RECOVERY_ZONE_NAMES_OPTION_DESC = "Names specifying zones to get partition states from. "
                + "Case-sensitive, without quotes, all zones if not set";

        public static final String RECOVERY_ZONE_NAME_OPTION = "--zone";

        public static final String RECOVERY_ZONE_NAME_OPTION_DESC = "Name of the zone to reset partitions of. "
                + "Case-sensitive, without quotes";

        public static final String RECOVERY_TABLE_NAME_OPTION = "--table";

        public static final String RECOVERY_TABLE_NAME_OPTION_DESC = "Fully-qualified name of the table to reset partitions of. "
                + "Case-sensitive, without quotes";

        public static final String RECOVERY_NODE_NAMES_OPTION = "--nodes";

        public static final String RECOVERY_NODE_NAMES_OPTION_DESC = "Names specifying nodes to get partition states from. "
                + "Case-sensitive, without quotes, all nodes if not set";

        public static final String RECOVERY_METASTORAGE_REPLICATION_OPTION = "--metastorage-replication-factor";

        public static final String RECOVERY_METASTORAGE_REPLICATION_DESC = "Number of nodes in the voting member set of the Metastorage "
                + "RAFT group.";

        public static final String RECOVERY_CMG_NODES_OPTION = "--cluster-management-group";

        public static final String RECOVERY_CMG_NODES_OPTION_DESC = "Names of nodes (use comma-separated list of node names "
                + "'--cluster-management-group node1, node2' "
                + "to specify more than one node) that will host the Cluster Management Group.";

        public static final String RECOVERY_WITH_CLEANUP_OPTION = "--with-cleanup";

        public static final String RECOVERY_WITH_CLEANUP_OPTION_DESC = "Restarts partitions, preceded by a storage cleanup. "
                + "This will remove all data from the partition storages before restart.";

        /** Old cluster endpoint URL option long name. */
        public static final String RECOVERY_OLD_CLUSTER_URL_OPTION = "--old-cluster-url";

        /** Old cluster endpoint URL option description. */
        public static final String RECOVERY_OLD_CLUSTER_URL_OPTION_DESC = "URL of old cluster endpoint (nodes of this cluster will be "
                + "migrated to a new cluster). It can be URL of any node of the old cluster.";

        /** New cluster endpoint URL option long name. */
        public static final String RECOVERY_NEW_CLUSTER_URL_OPTION = "--new-cluster-url";

        /** New cluster endpoint URL option description. */
        public static final String RECOVERY_NEW_CLUSTER_URL_OPTION_DESC = "URL of new cluster endpoint (nodes of old cluster will be "
                + "migrated to this cluster). It can be URL of any node of the new cluster.";

        public static final String CONFIG_FORMAT_OPTION = "--format";

        public static final String CONFIG_FORMAT_OPTION_DESC = "Output format. Valid values: ${COMPLETION-CANDIDATES}";

        /** Config update file option long name. */
        public static final String CONFIG_UPDATE_FILE_OPTION = "--file";

        /** Config update file option description. */
        public static final String CONFIG_UPDATE_FILE_OPTION_DESC = "Path to file with config update commands to execute";

        public static final String RESET_ZONE_NAMES_OPTION = "--zone-names";

        public static final String RESET_ZONE_NAMES_OPTION_DESC = "Comma-separated list of zone names to reset data nodes for. "
                + "If not specified, resets for all zones.";
    }
}
