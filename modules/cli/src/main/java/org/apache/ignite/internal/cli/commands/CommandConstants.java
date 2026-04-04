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

/**
 * Constants for @Command annotation.
 */
public class CommandConstants {
    public static final String DESCRIPTION_HEADING = "%nDESCRIPTION%n";
    public static final String OPTION_LIST_HEADING = "%nOPTIONS @|fg(246) * - required option|@ %n";
    public static final String SYNOPSIS_HEADING = "%nUSAGE%n";
    public static final String COMMAND_LIST_HEADING = "%nCOMMANDS%n";
    public static final String PARAMETER_LIST_HEADING = "%nPARAMETERS @|fg(246) * - required parameter|@ %n";
    public static final char REQUIRED_OPTION_MARKER = '*';
    public static final boolean USAGE_HELP_AUTO_WIDTH = true;
    public static final boolean SORT_OPTIONS = false;
    public static final boolean SORT_SYNOPSIS = false;
    public static final boolean ABBREVIATE_SYNOPSIS = true;
    public static final String FOOTER_HEADING = "%nEXAMPLES%n";

    public static final int CLUSTER_URL_OPTION_ORDER = 10;
    public static final int PROFILE_OPTION_ORDER = 11;
    public static final int HELP_OPTION_ORDER = 100;
    public static final int VERBOSE_OPTION_ORDER = 101;
    public static final int VERSION_OPTION_ORDER = 102;
}
