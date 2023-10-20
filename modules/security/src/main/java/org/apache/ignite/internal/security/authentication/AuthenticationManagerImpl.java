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

package org.apache.ignite.internal.security.authentication;

import static org.apache.ignite.internal.security.authentication.event.EventType.AUTHENTICATION_DISABLED;
import static org.apache.ignite.internal.security.authentication.event.EventType.AUTHENTICATION_ENABLED;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.security.authentication.configuration.AuthenticationProviderView;
import org.apache.ignite.internal.security.authentication.configuration.AuthenticationView;
import org.apache.ignite.internal.security.authentication.event.AuthenticationEvent;
import org.apache.ignite.internal.security.authentication.event.AuthenticationListener;
import org.apache.ignite.internal.security.authentication.event.AuthenticationProviderEvent;
import org.apache.ignite.internal.security.configuration.SecurityView;
import org.apache.ignite.security.exception.InvalidCredentialsException;
import org.apache.ignite.security.exception.UnsupportedAuthenticationTypeException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Implementation of {@link Authenticator}.
 */
public class AuthenticationManagerImpl implements AuthenticationManager {
    private static final IgniteLogger LOG = Loggers.forClass(AuthenticationManagerImpl.class);

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private final List<AuthenticationListener> listeners = new CopyOnWriteArrayList<>();

    private List<Authenticator> authenticators = new ArrayList<>();

    private boolean authEnabled = false;

    /**
     * {@inheritDoc}
     */
    @Override
    public UserDetails authenticate(AuthenticationRequest<?, ?> authenticationRequest) {
        rwLock.readLock().lock();
        try {
            if (authEnabled) {
                return authenticators.stream()
                        .map(authenticator -> authenticate(authenticator, authenticationRequest))
                        .filter(Objects::nonNull)
                        .findFirst()
                        .orElseThrow(() -> new InvalidCredentialsException("Authentication failed"));
            } else {
                return new UserDetails("Unknown", "Unknown");
            }
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Nullable
    private static UserDetails authenticate(Authenticator authenticator, AuthenticationRequest<?, ?> authenticationRequest) {
        try {
            return authenticator.authenticate(authenticationRequest);
        } catch (InvalidCredentialsException | UnsupportedAuthenticationTypeException exception) {
            return null;
        } catch (Exception e) {
            LOG.error("Unexpected exception during authentication", e);
            return null;
        }
    }

    @Override
    public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<SecurityView> ctx) {
        if (refreshProviders(ctx.newValue())) {
            emitEvents(ctx);
        }

        return CompletableFuture.completedFuture(null);
    }

    private boolean refreshProviders(@Nullable SecurityView view) {
        rwLock.writeLock().lock();
        try {
            if (view == null || !view.enabled()) {
                authEnabled = false;
                authenticators = List.of();
            } else if (view.enabled() && view.authentication().providers().size() != 0) {
                authenticators = providersFromAuthView(view.authentication());
                authEnabled = true;
            } else {
                LOG.error("Invalid configuration: security is enabled, but no providers. Leaving the old settings");

                return false;
            }

            return true;
        } catch (Exception exception) {
            LOG.error("Couldn't refresh authentication providers. Leaving the old settings", exception);

            return false;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private static List<Authenticator> providersFromAuthView(AuthenticationView view) {
        NamedListView<? extends AuthenticationProviderView> providers = view.providers();

        return providers.stream()
                .map(AuthenticatorFactory::create)
                .collect(Collectors.toList());
    }

    private void emitEvents(ConfigurationNotificationEvent<SecurityView> ctx) {
        SecurityView oldValue = ctx.oldValue();
        SecurityView newValue = ctx.newValue();

        // Authentication enabled/disabled.
        if ((oldValue == null || oldValue.enabled()) && !newValue.enabled()) {
            notifyListeners(() -> AUTHENTICATION_DISABLED);
        } else if ((oldValue == null || !oldValue.enabled()) && newValue.enabled()) {
            notifyListeners(() -> AUTHENTICATION_ENABLED);
        }

        if (oldValue != null) {
            // Authentication providers removed.
            oldValue.authentication()
                    .providers()
                    .stream()
                    .map(AuthenticationProviderView::name)
                    .filter(it -> newValue.authentication().providers().get(it) == null)
                    .map(AuthenticationProviderEvent::removed)
                    .forEach(this::notifyListeners);

            // Authentication providers updated.
            oldValue.authentication()
                    .providers()
                    .stream()
                    .filter(oldProvider -> {
                        AuthenticationProviderView newProvider = newValue.authentication().providers().get(oldProvider.name());
                        return newProvider != null && !AuthenticationProviderEqualityVerifier.areEqual(oldProvider, newProvider);
                    })
                    .map(AuthenticationProviderView::name)
                    .map(AuthenticationProviderEvent::updated)
                    .forEach(this::notifyListeners);
        }
    }

    private void notifyListeners(AuthenticationEvent event) {
        listeners.forEach(listener -> {
            try {
                listener.onEvent(event);
            } catch (Exception exception) {
                LOG.error("Couldn't notify listener", exception);
            }
        });
    }

    @Override
    public void listen(AuthenticationListener listener) {
        listeners.add(listener);
    }

    @Override
    public void stopListen(AuthenticationListener listener) {
        listeners.remove(listener);
    }

    @TestOnly
    public void authEnabled(boolean authEnabled) {
        this.authEnabled = authEnabled;
    }

    @TestOnly
    public void authenticators(List<Authenticator> authenticators) {
        this.authenticators = authenticators;
    }
}
