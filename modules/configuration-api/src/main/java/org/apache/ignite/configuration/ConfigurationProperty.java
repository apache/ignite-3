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

package org.apache.ignite.configuration;

import java.util.NoSuchElementException;
import org.apache.ignite.configuration.notifications.ConfigurationListener;

/**
 * Base interface for configuration.
 *
 * @param <VIEWT> Type of the value.
 */
public interface ConfigurationProperty<VIEWT> {
    /**
     * Get key of this node.
     */
    String key();

    /**
     * Get value of this property.
     */
    VIEWT value();

    /**
     * Adds configuration values listener.
     *
     * <p>NOTE: If this method is called from another listener, then it is guaranteed to be called starting from the next configuration
     * update only.
     *
     * @param listener Listener.
     */
    void listen(ConfigurationListener<VIEWT> listener);

    /**
     * Removes configuration values listener.
     *
     * <p>NOTE: Unpredictable behavior if the method is called inside other listeners.
     *
     * @param listener Listener.
     */
    void stopListen(ConfigurationListener<VIEWT> listener);

    /**
     * Returns a configuration tree for the purpose of reading configuration directly from the underlying storage. Actual reading is only
     * happening while invoking {@link ConfigurationTree#value()}. It will either throw {@link NoSuchElementException},
     * unchecked runtime exception, or return the value.
     *
     * <p>It is important to understand how it processes named list elements. Imagine having element named {@code a} with internalId
     * {@code aId}.
     * <pre><code>
     *     var namedListProxy = namedList.directProxy();
     *
     *     // Creates another proxy.
     *     var aElementProxy = namedListProxy.get("a");
     *
     *     // This operation performs actual reading. It'll throw an exception if element named "a" doesn't exist anymore.
     *     // It's been renamed or deleted.
     *     var aElement = aElementProxy.value();
     *
     *     // Creates another proxy.
     *     var aIdElementProxy = namedListProxy.get(aId);
     *
     *     // This operation performs actual reading as previously stated. But, unlike the access by name, it won't throw an exception in
     *     // case of a rename. Only after deletion.
     *     var aIdElement = aIdElementProxy.value();
     * </code></pre>
     *
     * <p>Another important case is how already resolved named list elements are being proxied.
     * <pre><code>
     *     // Following code is in fact equivalent to a "namedList.directProxy().get(aId);"
     *     // Already resolved elements are always referenced to by their internal ids. This means that proxy will return a valid value
     *     // even after rename despite it looking like name "a" should be resolved once again.
     *     var aElementProxy = namedList.get("a").directProxy();
     * </code></pre>
     */
    <T extends ConfigurationProperty<VIEWT>> T directProxy();
}
