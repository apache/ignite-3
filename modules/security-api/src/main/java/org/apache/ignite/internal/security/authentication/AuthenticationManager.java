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

import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.internal.security.authentication.event.AuthenticationListener;
import org.apache.ignite.internal.security.configuration.SecurityView;

/**
 * Authentication manager.
 */
public interface AuthenticationManager extends Authenticator, ConfigurationListener<SecurityView> {
    /**
     * Check if authentication is enabled.
     *
     * @return {@code true} if authentication is enabled.
     */
    boolean authenticationEnabled();

    /**
     * Listen to authentication events.
     *
     * @param listener Listener.
     */
    void listen(AuthenticationListener listener);

    /**
     * Stop listen to authentication events.
     *
     * @param listener Listener.
     */
    void stopListen(AuthenticationListener listener);
}
