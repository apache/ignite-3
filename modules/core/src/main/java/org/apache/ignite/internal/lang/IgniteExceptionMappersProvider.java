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

package org.apache.ignite.internal.lang;

import java.util.Collection;
import org.apache.ignite.lang.IgniteCheckedException;
import org.apache.ignite.lang.IgniteException;

/**
 * This interface provides the ability to register specific mappers from Ignite internal exceptions to a public ones.
 *
 * <p>
 *     Each module can provide such a mapping for its own internal exceptions.
 *     Designed for integration with {@link java.util.ServiceLoader} mechanism, so IgniteExceptionMapper instances
 *     provided by a library are to be defined as services either
 *     in {@code META-INF/services/org.apache.ignite.lang.IgniteExceptionMapper}, or in a {@code module-info.java}.
 * </p>
 *
 * <p>There are the following constraints that should be taken into account by a particular implementation of this interface:</p>
 * <ul>
 *     <li>it is prohibited to register more than one mapper for the same internal exception.</li>
 *     <li>mapper should only provide mappings either to {@link IgniteException}, or {@link IgniteCheckedException}.</li>
 *     <li>mapper should not provide mapping for Java standard exception like {@link NullPointerException},
 *     {@link IllegalArgumentException}, etc.</li>
 *     <li>mapper should not provide any mappings for errors {@link Error}.</li>
 * </ul>
 */
public interface IgniteExceptionMappersProvider {
    /**
     * Returns a collection of mappers to be used to map internal exceptions to public ones.
     *
     * @return Collection of mappers.
     */
    Collection<IgniteExceptionMapper<?, ?>> mappers();
}
