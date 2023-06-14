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

package org.apache.ignite.internal.app;

import picocli.CommandLine.IDefaultValueProvider;
import picocli.CommandLine.Model.ArgSpec;
import picocli.CommandLine.Model.OptionSpec;

/** Default value provider which gets values from environment. */
public class EnvironmentDefaultValueProvider implements IDefaultValueProvider {

    private static final String ENV_NAME_PREFIX = "IGNITE_";

    @Override
    public String defaultValue(ArgSpec argSpec) throws Exception {
        if (argSpec.isOption()) {
            OptionSpec optionSpec = (OptionSpec) argSpec;
            String longestName = optionSpec.longestName();
            if (longestName.startsWith("--")) {
                return System.getenv(getEnvName(longestName));
            }
        }
        return null;
    }

    /**
     * Gets the name of the environment variable corresponding to the option name.
     *
     * @param optionName Option name.
     * @return Environment variable name.
     */
    public static String getEnvName(String optionName) {
        return ENV_NAME_PREFIX + optionName.substring("--".length()).replace('-', '_').toUpperCase();
    }
}
