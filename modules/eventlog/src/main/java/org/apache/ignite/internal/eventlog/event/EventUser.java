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

package org.apache.ignite.internal.eventlog.event;

import java.util.Objects;

/** The user who initiated the event. */
public final class EventUser {
    private final String username;

    private final String authenticationProvider;

    private EventUser(String username, String authenticationProvider) {
        this.username = username;
        this.authenticationProvider = authenticationProvider;
    }

    /** Creates a new instance of the {@link EventUser} class. */
    public static EventUser of(String username, String authenticationProvider) {
        return new EventUser(username, authenticationProvider);
    }

    /** Creates an instance of the SYSTEM {@link EventUser} class. */
    public static EventUser system() {
        return of("SYSTEM", "SYSTEM");
    }

    /** The authentication provider for the user. For "SYSTEM" user it is also "SYSTEM". */
    public String authenticationProvider() {
        return authenticationProvider;
    }

    /** The username of the user. */
    public String username() {
        return username;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EventUser eventUser = (EventUser) o;
        return Objects.equals(username, eventUser.username) && Objects.equals(authenticationProvider,
                eventUser.authenticationProvider);
    }

    @Override
    public int hashCode() {
        return Objects.hash(username, authenticationProvider);
    }

    @Override
    public String toString() {
        return "EventUser{"
                + "username='" + username + '\''
                + ", authenticationProvider='" + authenticationProvider + '\''
                + '}';
    }
}
