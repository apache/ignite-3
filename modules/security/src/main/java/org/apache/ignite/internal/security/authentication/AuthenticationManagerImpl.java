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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.security.authentication.AuthenticationUtils.findBasicProviderName;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.eventlog.api.EventLog;
import org.apache.ignite.internal.eventlog.api.IgniteEvents;
import org.apache.ignite.internal.eventlog.event.EventUser;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
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
     * List of authenticators. Null when authentication is disabled.
     */
    private volatile @Nullable List<Authenticator> authenticators;

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
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
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
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
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
    public CompletableFuture<UserDetails> authenticateAsync(AuthenticationRequest<?, ?> authenticationRequest) {
        var providers = authenticators;

        if (providers != null) {
            return authenticate(providers.iterator(), authenticationRequest);
        } else {
            return completedFuture(UserDetails.UNKNOWN);
        }
    }

    private CompletableFuture<UserDetails> authenticate(Iterator<Authenticator> iter, AuthenticationRequest<?, ?> authenticationRequest) {
        if (!iter.hasNext()) {
            return failedFuture(new InvalidCredentialsException("Authentication failed"));
        }

        return authenticate(iter.next(), authenticationRequest)
                .thenCompose(userDetails -> {
                    if (userDetails != null) {
                        return completedFuture(userDetails);
                    }

                    return authenticate(iter, authenticationRequest);
                });
    }

    private CompletableFuture<UserDetails> authenticate(
            Authenticator authenticator,
            AuthenticationRequest<?, ?> authenticationRequest
    ) {
        try {
            var fut = authenticator.authenticateAsync(authenticationRequest);

            fut.thenAccept(userDetails -> {
                if (userDetails != null) {
                    logUserAuthenticated(userDetails);
                }
            });

            return fut.handle((userDetails, throwable) -> {
                if (throwable != null) {
                    if (!(throwable instanceof InvalidCredentialsException
                            || throwable instanceof UnsupportedAuthenticationTypeException)) {
                        LOG.error("Unexpected exception during authentication", throwable);
                    }

                    logAuthenticationFailure(authenticationRequest);
                    return null;
                }

                return userDetails;
            });
        } catch (InvalidCredentialsException | UnsupportedAuthenticationTypeException exception) {
            logAuthenticationFailure(authenticationRequest);
            return completedFuture(null);
        } catch (Exception e) {
            logAuthenticationFailure(authenticationRequest);
            LOG.error("Unexpected exception during authentication", e);
            return completedFuture(null);
        }
    }

    private void logAuthenticationFailure(AuthenticationRequest<?, ?> authenticationRequest) {
        eventLog.log(() ->
                IgniteEvents.USER_AUTHENTICATION_FAILURE.builder()
                        .user(EventUser.system())
                        .fields(Map.of("identity", tryGetUsernameOrUnknown(authenticationRequest)))
                        .build()
        );
    }

    private static String tryGetUsernameOrUnknown(AuthenticationRequest<?, ?> authenticationRequest) {
        if (authenticationRequest instanceof UsernamePasswordRequest) {
            return ((UsernamePasswordRequest) authenticationRequest).getIdentity();
        }
        return "UNKNOWN_AUTHENTICATION_TYPE";
    }

    private void logUserAuthenticated(UserDetails userDetails) {
        eventLog.log(() ->
                IgniteEvents.USER_AUTHENTICATION_SUCCESS.create(EventUser.of(
                        userDetails.username(), userDetails.providerName()
                )));
    }

    private void refreshProviders(@Nullable SecurityView view) {
        try {
            if (view == null || !view.enabled()) {
                authenticators = null;
            } else {
                authenticators = providersFromAuthView(view.authentication());
            }
        } catch (Exception exception) {
            LOG.error("Couldn't refresh authentication providers. Leaving the old settings", exception);
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
        return authenticators != null;
    }

    @TestOnly
    public void authenticators(List<Authenticator> authenticators) {
        this.authenticators = authenticators;
    }
}
