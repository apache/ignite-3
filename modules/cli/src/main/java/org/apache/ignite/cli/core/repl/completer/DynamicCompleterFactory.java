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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.micronaut.context.annotation.Bean;
import java.util.Set;
import org.apache.ignite.cli.call.configuration.ClusterConfigShowCall;
import org.apache.ignite.cli.call.configuration.ClusterConfigShowCallInput;
import org.apache.ignite.cli.call.configuration.NodeConfigShowCall;
import org.apache.ignite.cli.call.configuration.NodeConfigShowCallInput;

/**
 * Factory that creates {@link DynamicCompleter}s.
 */
@Bean
public class DynamicCompleterFactory {
    private final NodeConfigShowCall nodeConfigShowCall;
    private final ClusterConfigShowCall clusterConfigShowCall;
    private final NodeUrlProvider urlProvider;

    /** Default constructor. */
    public DynamicCompleterFactory(
            NodeConfigShowCall nodeConfigShowCall,
            ClusterConfigShowCall clusterConfigShowCall,
            NodeUrlProvider urlProvider) {

        this.nodeConfigShowCall = nodeConfigShowCall;
        this.clusterConfigShowCall = clusterConfigShowCall;
        this.urlProvider = urlProvider;
    }

    /** Creates node config completer with given activation prefix. */
    public LazyDynamicCompleter nodeConfigCompleter(String activationPrefix) {
        return nodeConfigCompleter(Set.of(activationPrefix));
    }

    /** Creates node config completer with given set of activation prefixes. */
    public LazyDynamicCompleter nodeConfigCompleter(Set<String> activationPrefixes) {
        return new LazyDynamicCompleter(() -> {
            try {
                Config config = ConfigFactory.parseString(
                        nodeConfigShowCall.execute(
                                // todo https://issues.apache.org/jira/browse/IGNITE-17416
                                NodeConfigShowCallInput.builder().nodeUrl(urlProvider.resolveUrl(new String[]{""})).build()
                        ).body().getValue()
                );
                return new HoconDynamicCompleter(activationPrefixes, config);
            } catch (Exception e) {
                return new HoconDynamicCompleter(activationPrefixes, ConfigFactory.parseString(""));
            }
        });
    }

    /** Creates cluster config completer with given activation prefix. */
    public LazyDynamicCompleter clusterConfigCompleter(String activationPrefix) {
        return clusterConfigCompleter(Set.of(activationPrefix));
    }

    /** Creates cluster config completer with given set of activation prefixes. */
    public LazyDynamicCompleter clusterConfigCompleter(Set<String> activationPrefixes) {
        return new LazyDynamicCompleter(() -> {
            try {
                Config config = ConfigFactory.parseString(
                        clusterConfigShowCall.execute(
                                // todo https://issues.apache.org/jira/browse/IGNITE-17416
                                ClusterConfigShowCallInput.builder().clusterUrl(urlProvider.resolveUrl(new String[]{""})).build()
                        ).body().getValue()
                );
                return new HoconDynamicCompleter(activationPrefixes, config);
            } catch (Exception e) {
                return new HoconDynamicCompleter(activationPrefixes, ConfigFactory.parseString(""));
            }
        });
    }
}
