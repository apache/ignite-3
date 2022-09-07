/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cli.commands;

import org.apache.ignite.cli.config.ConfigConstants;

/**
 * Constants to use in {@code Option} annotations for commands.
 */
public class OptionsConstants {
    /** Cluster endpoint URL option long name. */
    public static final String CLUSTER_URL_OPTION = "--cluster-endpoint-url";

    /** Cluster endpoint URL option description. */
    public static final String CLUSTER_URL_DESC = "URL of cluster endpoint";

    /** Cluster endpoint URL option description key. */
    public static final String CLUSTER_URL_KEY = ConfigConstants.CLUSTER_URL;

    /** Node URL option long name. */
    public static final String NODE_URL_OPTION = "--node-url";

    /** Node URL option description. */
    public static final String NODE_URL_DESC = "URL of ignite node";

    /** Profile name option names. */
    public static final String PROFILE_OPTION = "--profile";
    public static final String PROFILE_OPTION_SHORT = "-p";

    /** Profile name option description. */
    public static final String PROFILE_OPTION_DESC = "Profile name";

    /** URL option short name. */
    public static final String URL_OPTION_SHORT = "-u";
}
