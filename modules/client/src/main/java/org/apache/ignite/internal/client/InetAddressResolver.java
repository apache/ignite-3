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

package org.apache.ignite.internal.client;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * DNS resolver.
 */
@FunctionalInterface
interface InetAddressResolver {
    InetAddressResolver DEFAULT = InetAddress::getAllByName;

    /**
     * Resolves the given host name to its IP addresses.
     *
     * @param host the host name to be resolved
     * @return an array of {@code InetAddress} objects representing the IP addresses of the host
     * @throws UnknownHostException if the host name could not be resolved
     */
    InetAddress[] getAllByName(String host)
            throws UnknownHostException;
}
