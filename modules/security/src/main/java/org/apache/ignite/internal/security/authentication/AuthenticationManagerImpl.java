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

import static org.apache.ignite.internal.security.authentication.AuthenticationUtils.findBasicProviderName;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.eventlog.api.EventLog;
import org.apache.ignite.internal.eventlog.api.IgniteEvents;
import org.apache.ignite.internal.eventlog.event.EventUser;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.security.authentication.basic.BasicAuthenticationProviderConfiguration;
import org.apache.ignite.internal.security.authentication.configuration.AuthenticationProviderView;
import org.apache.ignite.internal.security.authentication.configuration.AuthenticationView;
import org.apache.ignite.internal.security.authentication.event.AuthenticationEvent;
import org.apache.ignite.internal.security.authentication.event.AuthenticationEventParameters;
import org.apache.ignite.internal.security.authentication.event.AuthenticationProviderEventFactory;
import org.apache.ignite.internal.security.authentication.event.SecurityEnabledDisabledEventFactory;
import org.apache.ignite.internal.security.authentication.event.UserEventFactory;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;
import org.apache.ignite.internal.security.configuration.SecurityView;
import org.apache.ignite.security.exception.InvalidCredentialsException;
import org.apache.ignite.security.exception.UnsupportedAuthenticationTypeException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Implementation of {@link Authenticator}.
 */
public class AuthenticationManagerImpl
        extends AbstractEventProducer<AuthenticationEvent, AuthenticationEventParameters>
        implements AuthenticationManager {
    private static final IgniteLogger LOG = Loggers.forClass(AuthenticationManagerImpl.class);

    /**
     * Security configuration.
     */
    private final SecurityConfiguration securityConfiguration;

    /**
     * Security configuration listener. Refreshes the list of authenticators when the configuration changes.
     */
    private final ConfigurationListener<SecurityView> securityConfigurationListener;

    /**
     * Security enabled/disabled event factory. Fires events when security is enabled/disabled.
     */
    private final SecurityEnabledDisabledEventFactory securityEnabledDisabledEventFactory;

    /**
     * User event factory. Fires events when a basic user is created/updated/deleted.
     */
    private final UserEventFactory userEventFactory;

    /**
     * Authentication provider event factory. Fires events when an authentication provider is created/updated/deleted.
     */
    private final AuthenticationProviderEventFactory providerEventFactory;

    /**
     * Read-write lock for the list of authenticators and the authentication enabled flag.
     */
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    /**
     * List of authenticators.
     */
    private List<Authenticator> authenticators = new ArrayList<>();

    /**
     * Authentication enabled flag.
     */
    private boolean authEnabled = false;

    private final EventLog eventLog;

    /**
     * Constructor.
     *
     * @param securityConfiguration Security configuration.
     * @param eventLog Event log.
     */
    public AuthenticationManagerImpl(SecurityConfiguration securityConfiguration, EventLog eventLog) {
        this.securityConfiguration = securityConfiguration;
        this.eventLog = eventLog;

        securityConfigurationListener = ctx -> {
            refreshProviders(ctx.newValue());
            return nullCompletedFuture();
        };

        securityEnabledDisabledEventFactory = new SecurityEnabledDisabledEventFactory(this::fireEvent);

        userEventFactory = new UserEventFactory(this::fireEvent);

        providerEventFactory = new AuthenticationProviderEventFactory(
                securityConfiguration,
                userEventFactory,
                this::fireEvent
        );
    }

    @Override
    public CompletableFuture<Void> startAsync(ExecutorService startupExecutor) {
        securityConfiguration.listen(securityConfigurationListener);
        securityConfiguration.enabled().listen(securityEnabledDisabledEventFactory);
        securityConfiguration.authentication().providers().listenElements(providerEventFactory);

        String basicAuthenticationProviderName = findBasicProviderName(securityConfiguration.authentication().providers().value());
        BasicAuthenticationProviderConfiguration basicAuthenticationProviderConfiguration = (BasicAuthenticationProviderConfiguration)
                securityConfiguration.authentication().providers().get(basicAuthenticationProviderName);
        basicAuthenticationProviderConfiguration.users().listenElements(userEventFactory);

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ExecutorService stopExecutor) {
        securityConfiguration.stopListen(securityConfigurationListener);
        securityConfiguration.enabled().stopListen(securityEnabledDisabledEventFactory);
        securityConfiguration.authentication().providers().stopListenElements(providerEventFactory);

        String basicAuthenticationProviderName = findBasicProviderName(securityConfiguration.authentication().providers().value());
        BasicAuthenticationProviderConfiguration basicAuthenticationProviderConfiguration = (BasicAuthenticationProviderConfiguration)
                securityConfiguration.authentication().providers().get(basicAuthenticationProviderName);
        basicAuthenticationProviderConfiguration.users().stopListenElements(userEventFactory);

        return nullCompletedFuture();
    }

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
                        .map(userDetails ->  {
                            eventLog.log(() ->
                                    IgniteEvents.USER_AUTHENTICATED.create(EventUser.of(
                                            userDetails.username(), userDetails.providerName()
                                    )));

                            return userDetails;
                        })
                        .orElseThrow(() -> new InvalidCredentialsException("Authentication failed"));
            } else {
                return UserDetails.UNKNOWN;
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

    private void refreshProviders(@Nullable SecurityView view) {
        rwLock.writeLock().lock();
        try {
            if (view == null || !view.enabled()) {
                authEnabled = false;
            } else {
                authenticators = providersFromAuthView(view.authentication());
                authEnabled = true;
            }
        } catch (Exception exception) {
            LOG.error("Couldn't refresh authentication providers. Leaving the old settings", exception);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private static List<Authenticator> providersFromAuthView(AuthenticationView view) {
        NamedListView<? extends AuthenticationProviderView> providers = view.providers();

        return providers.stream()
                .sorted(Comparator.comparing((AuthenticationProviderView o) -> o.name()))
                .map(AuthenticatorFactory::create)
                .collect(Collectors.toList());
    }

    private CompletableFuture<Void> fireEvent(AuthenticationEventParameters parameters) {
        return fireEvent(parameters.type(), parameters);
    }


    @Override
    public boolean authenticationEnabled() {
        return authEnabled;
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
