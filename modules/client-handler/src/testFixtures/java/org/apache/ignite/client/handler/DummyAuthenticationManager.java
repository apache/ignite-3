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

package org.apache.ignite.client.handler;

import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.security.authentication.AuthenticationManager;
import org.apache.ignite.internal.security.authentication.AuthenticationRequest;
import org.apache.ignite.internal.security.authentication.UserDetails;
import org.apache.ignite.internal.security.authentication.event.AuthenticationEvent;
import org.apache.ignite.internal.security.authentication.event.AuthenticationEventParameters;

/**
 * Dummy authentication manager that always returns {@link UserDetails#UNKNOWN}.
 */
public class DummyAuthenticationManager implements AuthenticationManager {
    @Override
    public void listen(AuthenticationEvent evt, EventListener<? extends AuthenticationEventParameters> listener) {

    }

    @Override
    public void removeListener(AuthenticationEvent evt, EventListener<? extends AuthenticationEventParameters> listener) {

    }

    @Override
    public void start() {

    }

    @Override
    public void stop() throws Exception {

    }

    @Override
    public boolean authenticationEnabled() {
        return false;
    }

    @Override
    public UserDetails authenticate(AuthenticationRequest<?, ?> authenticationRequest) {
        return UserDetails.UNKNOWN;
    }
}
