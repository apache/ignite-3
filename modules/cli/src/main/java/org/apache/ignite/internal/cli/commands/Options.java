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

import org.apache.ignite.internal.cli.config.CliConfigKeys;

/**
 * Constants to use in {@code Option} annotations for commands.
 */
public enum Options {
    CLUSTER_URL(Constants.CLUSTER_URL_OPTION, Constants.URL_OPTION_SHORT, Constants.CLUSTER_URL_OPTION_DESC),
    NODE_URL(Constants.NODE_URL_OPTION, Constants.URL_OPTION_SHORT, Constants.NODE_URL_OPTION_DESC),

    CLUSTER_NAME(Constants.CLUSTER_NAME_OPTION, Constants.CLUSTER_NAME_OPTION_SHORT, Constants.CLUSTER_NAME_OPTION_DESC),
    NODE_NAME(Constants.NODE_NAME_OPTION, Constants.NODE_NAME_OPTION_SHORT, Constants.NODE_NAME_OPTION_DESC),

    CMG_NODE_NAME(Constants.CMG_NODE_NAME_OPTION, Constants.CMG_NODE_NAME_OPTION_SHORT, Constants.CMG_NODE_NAME_OPTION_DESC),
    META_STORAGE_NODE_NAME(
            Constants.META_STORAGE_NODE_NAME_OPTION,
            Constants.META_STORAGE_NODE_NAME_OPTION_SHORT,
            Constants.META_STORAGE_NODE_NAME_OPTION_DESC
    ),

    PROFILE(Constants.PROFILE_OPTION, Constants.PROFILE_OPTION_SHORT, Constants.PROFILE_OPTION_DESC),
    PROFILE_COPY_FROM(
            Constants.PROFILE_COPY_FROM_OPTION,
            Constants.PROFILE_COPY_FROM_OPTION_SHORT,
            Constants.PROFILE_COPY_FROM_OPTION_DESC
    ),
    PROFILE_ACTIVATE(Constants.PROFILE_ACTIVATE_OPTION, Constants.PROFILE_ACTIVATE_OPTION_SHORT, Constants.PROFILE_ACTIVATE_OPTION_DESC),

    SCRIPT_FILE(Constants.SCRIPT_FILE_OPTION, Constants.SCRIPT_FILE_OPTION_SHORT, Constants.SCRIPT_FILE_OPTION_DESC),
    JDBC_URL(Constants.JDBC_URL_OPTION, Constants.JDBC_URL_OPTION_SHORT, Constants.JDBC_URL_OPTION_DESC),

    UNIT_PATH(Constants.UNIT_PATH_OPTION, Constants.UNIT_PATH_OPTION_SHORT, Constants.UNIT_PATH_OPTION_DESC),
    UNIT_VERSION(Constants.VERSION_OPTION, Constants.UNIT_VERSION_OPTION_SHORT, Constants.UNIT_VERSION_OPTION_DESC),

    PLAIN(Constants.PLAIN_OPTION, Constants.PLAIN_OPTION, Constants.PLAIN_OPTION_DESC),
    VERBOSE(Constants.VERBOSE_OPTION, Constants.VERBOSE_OPTION_SHORT, Constants.VERBOSE_OPTION_DESC),
    HELP(Constants.HELP_OPTION, Constants.HELP_OPTION_SHORT, Constants.HELP_OPTION_DESC),
    VERSION(Constants.VERSION_OPTION, Constants.VERSION_OPTION, Constants.VERSION_OPTION_DESC),
    AUTHENTICATION_ENABLED(
            Constants.AUTHENTICATION_ENABLED_OPTION,
            Constants.AUTHENTICATION_ENABLED_OPTION_SHORT,
            Constants.AUTHENTICATION_ENABLED_OPTION_DESC
    ),
    BASIC_AUTHENTICATION_LOGIN(
            Constants.BASIC_AUTHENTICATION_LOGIN_OPTION,
            Constants.BASIC_AUTHENTICATION_LOGIN_OPTION_SHORT,
            Constants.BASIC_AUTHENTICATION_LOGIN_OPTION_DESC
    ),
    BASIC_AUTHENTICATION_PASSWORD(
            Constants.BASIC_AUTHENTICATION_PASSWORD_OPTION,
            Constants.BASIC_AUTHENTICATION_PASSWORD_OPTION_SHORT,
            Constants.BASIC_AUTHENTICATION_PASSWORD_OPTION_DESC
    );

    private final String fullName;
    private final String shortName;
    private final String description;

    Options(String fullName, String shortName, String description) {
        this.fullName = fullName;
        this.shortName = shortName;
        this.description = description;
    }

    public String fullName() {
        return fullName;
    }

    public String shortName() {
        return shortName;
    }

    public String description() {
        return description;
    }

    /** Constants for all options. */
    public static final class Constants {
        /** Cluster endpoint URL option long name. */
        public static final String CLUSTER_URL_OPTION = "--cluster-endpoint-url";

        /** Cluster endpoint URL option description. */
        public static final String CLUSTER_URL_OPTION_DESC = "URL of cluster endpoint";

        /** Cluster endpoint URL option description key. */
        public static final String CLUSTER_URL_KEY = CliConfigKeys.Constants.CLUSTER_URL;

        /** Node URL option long name. */
        public static final String NODE_URL_OPTION = "--node-url";

        /** Node URL option description. */
        public static final String NODE_URL_OPTION_DESC = "URL of ignite node";

        /** Node URL or name option description. */
        public static final String NODE_URL_OR_NAME_DESC = "URL or name of an Ignite node";

        /** Profile name option long name. */
        public static final String PROFILE_OPTION = "--profile";

        /** Profile name option short name. */
        public static final String PROFILE_OPTION_SHORT = "-p";

        /** Profile name option description. */
        public static final String PROFILE_OPTION_DESC = "Profile name";

        /** URL option short name. */
        public static final String URL_OPTION_SHORT = "-u";

        /** Node name option long name. */
        public static final String NODE_NAME_OPTION = "--node-name";

        /** Node name option short name. */
        public static final String NODE_NAME_OPTION_SHORT = "-n";

        /** Node name option description. */
        public static final String NODE_NAME_OPTION_DESC = "Name of an Ignite node";

        /** Verbose option long name. */
        public static final String VERBOSE_OPTION = "--verbose";

        /** Verbose option short name. */
        public static final String VERBOSE_OPTION_SHORT = "-v";

        /** Verbose option description. */
        public static final String VERBOSE_OPTION_DESC = "Show additional information: logs, REST calls";

        /** Help option long name. */
        public static final String HELP_OPTION = "--help";

        /** Help option short name. */
        public static final String HELP_OPTION_SHORT = "-h";

        /** Verbose option description. */
        public static final String HELP_OPTION_DESC = "Show help for the specified command";

        /** Profile copy from option long name. */
        public static final String PROFILE_COPY_FROM_OPTION = "--copy-from";

        /** Profile copy from option short name. */
        public static final String PROFILE_COPY_FROM_OPTION_SHORT = "-c";

        /** Profile copy from option description. */
        public static final String PROFILE_COPY_FROM_OPTION_DESC = "Profile whose content will be copied to new one";

        /** Profile activate option long name. */
        public static final String PROFILE_ACTIVATE_OPTION = "--activate";

        /** Profile activate option short name. */
        public static final String PROFILE_ACTIVATE_OPTION_SHORT = "-a";

        /** Profile activate option description. */
        public static final String PROFILE_ACTIVATE_OPTION_DESC = "Activate new profile as current or not";

        /** Cluster management node name option long name. */
        public static final String CMG_NODE_NAME_OPTION = "--cmg-node";

        /** Cluster management node name option short name. */
        public static final String CMG_NODE_NAME_OPTION_SHORT = "-c";

        /** Cluster management node name option description. */
        public static final String CMG_NODE_NAME_OPTION_DESC = "Name of the node (repeat like '--cmg-node node1 --cmg-node node2' "
                + "to specify more than one node) that will host the Cluster Management Group."
                + "If omitted, then --meta-storage-node values will also supply the nodes for the Cluster Management Group.";

        /** Meta storage management node name option long name. */
        public static final String META_STORAGE_NODE_NAME_OPTION = "--meta-storage-node";

        /** Meta storage node name option short name. */
        public static final String META_STORAGE_NODE_NAME_OPTION_SHORT = "-m";

        /** Meta storage node name option description. */
        public static final String META_STORAGE_NODE_NAME_OPTION_DESC = "Name of the node (repeat like '--meta-storage-node node1 "
                + "--meta-storage-node node2' to specify more than one node) that will host the Meta Storage."
                + "If the --cmg-node parameter is omitted, the same nodes will also host the Cluster Management Group.";

        /** Cluster name option long name. */
        public static final String CLUSTER_NAME_OPTION = "--cluster-name";

        /** Cluster name option short name. */
        public static final String CLUSTER_NAME_OPTION_SHORT = "-n";

        /** Cluster name option description. */
        public static final String CLUSTER_NAME_OPTION_DESC = "Human-readable name of the cluster";

        /** Plain option long name. */
        public static final String PLAIN_OPTION = "--plain";

        /** Plain option description. */
        public static final String PLAIN_OPTION_DESC = "Display output with plain formatting";

        /** JDBC URL option long name. */
        public static final String JDBC_URL_OPTION = "--jdbc-url";

        /** JDBC URL option short name. */
        public static final String JDBC_URL_OPTION_SHORT = "-u";

        /** JDBC URL option description. */
        public static final String JDBC_URL_OPTION_DESC = "JDBC url to ignite cluster";

        /** SQL script file option long name. */
        public static final String SCRIPT_FILE_OPTION = "--script-file";

        /** SQL script file option short name. */
        public static final String SCRIPT_FILE_OPTION_SHORT = "-f";

        /** SQL script file option description. */
        public static final String SCRIPT_FILE_OPTION_DESC = "Path to file with SQL commands to execute";

        /** Version option long name. */
        public static final String VERSION_OPTION = "--version";

        /** Version option description. */
        public static final String VERSION_OPTION_DESC = "Print version information and exit";

        /** Version option short name. */
        public static final String UNIT_VERSION_OPTION_SHORT = "-uv";

        /** Unit version option description. */
        public static final String UNIT_VERSION_OPTION_DESC = "Unit version (x.y.z)";

        /** Path option long name. */
        public static final String UNIT_PATH_OPTION = "--path";

        /** Unit path option short name. */
        public static final String UNIT_PATH_OPTION_SHORT = "-up";

        /** Unit path option description. */
        public static final String UNIT_PATH_OPTION_DESC = "Path to deployment unit file or directory";

        public static final String AUTHENTICATION_ENABLED_OPTION = "--auth-enabled";

        public static final String AUTHENTICATION_ENABLED_OPTION_SHORT = "-ae";

        public static final String AUTHENTICATION_ENABLED_OPTION_DESC = "Authentication enabled flag";

        public static final String BASIC_AUTHENTICATION_LOGIN_OPTION = "--basic-auth-login";

        public static final String BASIC_AUTHENTICATION_LOGIN_OPTION_SHORT = "-bl";

        public static final String BASIC_AUTHENTICATION_LOGIN_OPTION_DESC = "Login which will be used for basic authentication";

        public static final String BASIC_AUTHENTICATION_PASSWORD_OPTION = "--basic-auth-password";

        public static final String BASIC_AUTHENTICATION_PASSWORD_OPTION_SHORT = "-bp";

        public static final String BASIC_AUTHENTICATION_PASSWORD_OPTION_DESC = "Password which will be used for basic authentication";
    }
}
