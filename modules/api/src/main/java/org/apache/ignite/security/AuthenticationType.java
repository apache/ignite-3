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

package org.apache.ignite.security;

import java.util.Arrays;
import org.apache.ignite.security.exception.UnsupportedAuthenticationTypeException;

/** Authentication types. */
public enum AuthenticationType {
    BASIC;

    /**
     * Parses {@link AuthenticationType} from the given string.
     *
     * @param type String representation of the type.
     * @return parsed {@link AuthenticationType}.
     * @throws UnsupportedAuthenticationTypeException in case of unknown type.
     */
    public static AuthenticationType parse(String type) {
        return Arrays.stream(values())
                .filter(it -> type.equalsIgnoreCase(it.name()))
                .findFirst()
                .orElseThrow(() -> new UnsupportedAuthenticationTypeException("Unknown authentication type: " + type));
    }
}
