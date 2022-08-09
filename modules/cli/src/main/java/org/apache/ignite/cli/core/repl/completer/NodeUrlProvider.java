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

package org.apache.ignite.cli.core.repl.completer;

import jakarta.inject.Singleton;
import org.apache.ignite.cli.commands.OptionsConstants;
import org.apache.ignite.cli.config.ConfigConstants;
import org.apache.ignite.cli.config.ConfigManagerProvider;
import org.apache.ignite.cli.core.repl.Session;

/**
 * Url provider for completer.
 */
@Singleton
public class NodeUrlProvider {

    private final Session session;
    private final ConfigManagerProvider configManagerProvider;

    /** Default constructor. */
    public NodeUrlProvider(Session session, ConfigManagerProvider configManagerProvider) {
        this.session = session;
        this.configManagerProvider = configManagerProvider;
    }

    /** Resolves the url for given words. */
    public String resolveUrl(String[] words) {
        String urlInWords = findClusterUrlIn(words);
        if (urlInWords != null) {
            return urlInWords;
        }

        if (session.isConnectedToNode()) {
            return session.nodeUrl();
        }

        return configManagerProvider.get().getCurrentProperty(ConfigConstants.CLUSTER_URL);
    }

    private String findClusterUrlIn(String[] words) {
        for (String word : words) {
            String prefix = OptionsConstants.CLUSTER_URL_OPTION + "=";
            if (word.startsWith(prefix)) {
                return word.substring(prefix.length());
            }
        }
        return null;
    }
}
