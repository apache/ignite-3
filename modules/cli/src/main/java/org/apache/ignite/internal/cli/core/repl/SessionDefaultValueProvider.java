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

package org.apache.ignite.internal.cli.core.repl;

import static org.apache.ignite.internal.cli.commands.Options.Constants.JDBC_URL_KEY;

import jakarta.inject.Singleton;
import org.apache.ignite.internal.cli.config.ConfigDefaultValueProvider;
import picocli.CommandLine.IDefaultValueProvider;
import picocli.CommandLine.Model.ArgSpec;

/**
 * Implementation of {@link IDefaultValueProvider} based on {@link Session}.
 */
@Singleton
public class SessionDefaultValueProvider implements IDefaultValueProvider {

    private final Session session;

    private final ConfigDefaultValueProvider defaultValueProvider;

    /**
     * Constructor.
     *
     * @param session session instance.
     * @param defaultValueProvider defaults value provider.
     */
    public SessionDefaultValueProvider(Session session, ConfigDefaultValueProvider defaultValueProvider) {
        this.session = session;
        this.defaultValueProvider = defaultValueProvider;
    }

    @Override
    public String defaultValue(ArgSpec argSpec) throws Exception {
        SessionInfo sessionInfo = session.info();
        if (sessionInfo != null) {
            if (JDBC_URL_KEY.equals(argSpec.descriptionKey())) {
                return sessionInfo.jdbcUrl();
            }
        }
        return defaultValueProvider.defaultValue(argSpec);
    }
}
