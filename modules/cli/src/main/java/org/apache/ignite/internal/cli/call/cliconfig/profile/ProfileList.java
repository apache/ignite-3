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

package org.apache.ignite.internal.cli.call.cliconfig.profile;

import java.util.Collection;

/**
 * Holds the list of profile names for decorating the output of the profile list command.
 */
public class ProfileList {
    private final Collection<String> profileNames;
    private final String currentProfileName;

    /**
     * Creates an instance with provided profile list.
     *
     * @param profileNames profile list
     * @param currentProfileName current profile name
     */
    public ProfileList(Collection<String> profileNames, String currentProfileName) {
        this.profileNames = profileNames;
        this.currentProfileName = currentProfileName;
    }

    /**
     * Gets profile list.
     *
     * @return profile list
     */
    public Collection<String> getProfileNames() {
        return profileNames;
    }

    /**
     * Gets current profile name.
     *
     * @return current profile name
     */
    public String getCurrentProfileName() {
        return currentProfileName;
    }
}
