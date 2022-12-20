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

package org.apache.ignite.internal.cli.core.repl.completer.hocon;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import jakarta.inject.Singleton;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.cli.call.configuration.ClusterConfigShowCall;
import org.apache.ignite.internal.cli.call.configuration.ClusterConfigShowCallInput;
import org.apache.ignite.internal.cli.core.repl.completer.DummyCompleter;
import org.apache.ignite.internal.cli.core.repl.completer.DynamicCompleter;
import org.apache.ignite.internal.cli.core.repl.completer.DynamicCompleterFactory;
import org.apache.ignite.internal.cli.core.repl.completer.NodeUrlProvider;

/** Factory for cluster config show/update command. */
@Singleton
public class ClusterConfigDynamicCompleterFactory implements DynamicCompleterFactory {

    private final NodeUrlProvider urlProvider;

    private final ClusterConfigShowCall clusterConfigShowCall;

    private final AtomicReference<DynamicCompleter> cachedCompleter = new AtomicReference<>(null);

    public ClusterConfigDynamicCompleterFactory(NodeUrlProvider urlProvider, ClusterConfigShowCall clusterConfigShowCall) {
        this.urlProvider = urlProvider;
        this.clusterConfigShowCall = clusterConfigShowCall;
    }

    @Override
    public DynamicCompleter getDynamicCompleter(String[] words) {
        if (cachedCompleter.getAcquire() != null) {
            return cachedCompleter.getAcquire();
        } else {
            // call REST on the background in order not to freeze UI thread
            CompletableFuture.runAsync(() -> {
                try {
                    Config config = ConfigFactory.parseString(
                            clusterConfigShowCall.execute(
                                    // todo https://issues.apache.org/jira/browse/IGNITE-17416
                                    ClusterConfigShowCallInput.builder().clusterUrl(urlProvider.resolveUrl(new String[]{""})).build()
                            ).body().getValue()
                    );
                    cachedCompleter.compareAndExchangeRelease(null, new HoconDynamicCompleter(config));
                } catch (Exception ignored) {
                    // no-op
                }
            });
        }

        // return dummy completer this time, but hope the next call will return cached completer
        return new DummyCompleter();
    }
}
