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

package org.apache.ignite.lang.util;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.lang.TraceableException;
import org.jetbrains.annotations.Nullable;

/**
 * This utility class allows to extract trace identifier from an instance of {@link TraceableException} or creating a new trace identifier.
 **/
public class TraceIdUtils {
    /**
     * Returns trace identifier from the provided throwable if it is an instance of {@link TraceableException}
     * or it has a cause that is an instance of {@link TraceableException}. Otherwise a new trace identifier is generated.
     *
     * @param t Throwable to extract a trace identifier.
     * @return Returns trace identifier.
     */
    public static UUID getOrCreateTraceId(@Nullable Throwable t) {
        Throwable e = t;

        // This collection is used to avoid infinite loops in case of cyclic dependencies.
        Set<Throwable> dejaVu = Collections.newSetFromMap(new IdentityHashMap<>());

        while (e != null) {
            if (e instanceof TraceableException) {
                return ((TraceableException) e).traceId();
            }
            if (!dejaVu.add(e)) {
                break;
            }
            e = e.getCause();
        }

        return UUID.randomUUID();
    }
}
