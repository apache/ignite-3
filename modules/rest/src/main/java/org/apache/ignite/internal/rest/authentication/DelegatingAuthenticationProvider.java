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

package org.apache.ignite.internal.rest.authentication;

import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.AuthenticationProvider;
import io.micronaut.security.authentication.AuthenticationRequest;
import io.micronaut.security.authentication.AuthenticationResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.rest.configuration.AuthenticationConfiguration;
import org.apache.ignite.internal.rest.configuration.AuthenticationProviderView;
import org.apache.ignite.internal.rest.configuration.AuthenticationView;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 * Implementation of {@link AuthenticationProvider}. Creates a list of {@link Authenticator} according to provided
 * {@link AuthenticationConfiguration} and updates them on configuration changes. Delegates {@link AuthenticationRequest} to the list of
 * {@link Authenticator}.
 */
public class DelegatingAuthenticationProvider implements AuthenticationProvider, ConfigurationListener<AuthenticationView> {

    private static final IgniteLogger LOG = Loggers.forClass(DelegatingAuthenticationProvider.class);

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final List<Authenticator> authenticators = new ArrayList<>();
    private boolean authEnabled = false;

    @Override
    public Publisher<AuthenticationResponse> authenticate(HttpRequest<?> httpRequest, AuthenticationRequest<?, ?> authenticationRequest) {
        return Flux.create(emitter -> {
            rwLock.readLock().lock();
            try {
                if (authEnabled) {
                    Optional<AuthenticationResponse> successResponse = authenticators.stream()
                            .map(it -> it.authenticate(authenticationRequest))
                            .filter(AuthenticationResponse::isAuthenticated)
                            .findFirst();
                    if (successResponse.isPresent()) {
                        emitter.next(successResponse.get());
                        emitter.complete();
                    } else {
                        emitter.error(AuthenticationResponse.exception());
                    }
                } else {
                    emitter.next(AuthenticationResponse.success("Unknown"));
                    emitter.complete();
                }
            } finally {
                rwLock.readLock().unlock();
            }
        }, FluxSink.OverflowStrategy.ERROR);
    }

    @Override
    public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<AuthenticationView> ctx) {
        return CompletableFuture.runAsync(() -> refreshProviders(ctx.newValue()));
    }

    private void refreshProviders(@Nullable AuthenticationView view) {
        rwLock.writeLock().lock();
        try {
            if (view == null || !view.enabled()) {
                authEnabled = false;
                authenticators.clear();
            } else if (view.enabled() && view.providers().size() != 0) {
                authenticators.clear();
                authenticators.addAll(providersFromAuthView(view));
                authEnabled = true;
            } else {
                LOG.error("Invalid configuration: auth enabled, but no providers. Leave the old settings");
            }
        } catch (Exception exception) {
            LOG.error("Couldn't refresh authentication providers. Leave the old settings", exception);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private static List<Authenticator> providersFromAuthView(AuthenticationView view) {
        NamedListView<? extends AuthenticationProviderView> providers = view.providers();
        return providers.namedListKeys().stream()
                .map(providers::get)
                .map(AuthenticatorFactory::create)
                .collect(Collectors.toList());
    }
}
