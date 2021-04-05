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

package org.apache.ignite.configuration.internal;

import org.apache.ignite.configuration.ConfigurationRegistry;

public class ConfigurationManagerImpl implements ConfigurationManager {
    private final ConfigurationManager locConfigurationMgr;
    private final ConfigurationManager distributedConfigurationMgr;

    public ConfigurationManagerImpl(
        ConfigurationManager locConfigurationMgr,
        ConfigurationManager distributedConfigurationMgr)
    {
        this.locConfigurationMgr = locConfigurationMgr;
        this.distributedConfigurationMgr = distributedConfigurationMgr;
    }

    @Override public void bootstrap(String jsonStr) {
        // TODO sanpwc: here some sort of routing should be handled.
    }

    @Override public ConfigurationRegistry configurationRegistry() {
        // TODO sanpwc: here some sort of routing should be handled.

        return null;
    }
}
