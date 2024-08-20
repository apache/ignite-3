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

package org.apache.ignite.internal.restart;

import java.util.function.Function;
import org.apache.ignite.Ignite;

/**
 * Cache for a reference obtained from some {@link Ignite} instance. Keeps the returned reference aligned with current Ignite
 * (i.e. if a restart happened, then the object is recreated before being returned).
 */
class RefCache<T> {
    private final Function<Ignite, T> supplier;

    // TODO: IGNITE-23005 - do not hold references to components of a stopped Ignite forever.
    private IgniteAware<T> value;

    RefCache(Ignite initialIgnite, Function<Ignite, T> supplier) {
        value = new IgniteAware<>(supplier.apply(initialIgnite), initialIgnite);
        this.supplier = supplier;
    }

    /**
     * Returns an instance corresponding to the given {@link Ignite} instance.
     *
     * <p>This method must ALWAYS be called under the attachment lock.
     *
     * @param ignite Ignite instance for which to obtain an object.
     */
    public T actualFor(Ignite ignite) {
        IgniteAware<T> igniteAware = this.value;

        if (igniteAware.ignite() != ignite) {
            igniteAware = new IgniteAware<>(supplier.apply(ignite), ignite);
            this.value = igniteAware;
        }

        return igniteAware.instance();
    }
}
