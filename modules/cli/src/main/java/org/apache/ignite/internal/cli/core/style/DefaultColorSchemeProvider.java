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

package org.apache.ignite.internal.cli.core.style;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;

/**
 * Default implementation of {@link ColorSchemeProvider} that reads from CLI configuration.
 */
@Singleton
public class DefaultColorSchemeProvider implements ColorSchemeProvider {
    private final ConfigManagerProvider configManagerProvider;

    @Inject
    public DefaultColorSchemeProvider(ConfigManagerProvider configManagerProvider) {
        this.configManagerProvider = configManagerProvider;
    }

    @Override
    public ColorScheme colorScheme() {
        String schemeName = configManagerProvider.get().getCurrentProperty(CliConfigKeys.COLOR_SCHEME.value());
        ColorScheme scheme = ColorScheme.fromString(schemeName);
        return scheme != null ? scheme : ColorScheme.DARK;
    }
}
