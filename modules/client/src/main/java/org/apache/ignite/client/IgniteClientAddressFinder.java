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

package org.apache.ignite.client;

/**
 * Provides a list of Ignite node addresses.
 *
 * <p>Unlike {@link IgniteClientConfiguration#addresses()}, this interface allows to provide addresses dynamically and handle
 * changes in the cluster topology.
 *
 * <p>Ignite client will periodically call {@link #getAddresses()} method to get the actual list of addresses.
 */
@SuppressWarnings("InterfaceMayBeAnnotatedFunctional")
public interface IgniteClientAddressFinder {
    /**
     * Get addresses of Ignite server nodes within a cluster. An address can be an IP address or a hostname, with or without port.
     * If port is not set then Ignite will use the default port - {@link IgniteClientConfiguration#DFLT_PORT}.
     *
     * @return Addresses of Ignite server nodes within a cluster.
     */
    public String[] getAddresses();
}
