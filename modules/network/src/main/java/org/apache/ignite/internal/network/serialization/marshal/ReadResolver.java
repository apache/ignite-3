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

package org.apache.ignite.internal.network.serialization.marshal;

import java.io.Externalizable;
import java.io.Serializable;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.SpecialMethodInvocationException;

/**
 * Resolves objects using {@code readResolve()} method.
 *
 * @see Serializable
 * @see Externalizable
 */
class ReadResolver {
    private final SchemaMismatchHandlers schemaMismatchHandlers;

    ReadResolver(SchemaMismatchHandlers schemaMismatchHandlers) {
        this.schemaMismatchHandlers = schemaMismatchHandlers;
    }

    Object applyReadResolveIfNeeded(Object object, ClassDescriptor remoteDescriptor) throws UnmarshalException {
        ClassDescriptor localDescriptor = remoteDescriptor.local();

        if (remoteDescriptor.hasReadResolve() && localDescriptor.hasReadResolve()) {
            return applyReadResolve(object, localDescriptor);
        } else if (!remoteDescriptor.hasReadResolve() && localDescriptor.hasReadResolve()) {
            return applyReadResolveIfSchemaMismatchHandlerAllows(object, localDescriptor);
        } else if (remoteDescriptor.hasReadResolve() && !localDescriptor.hasReadResolve()) {
            schemaMismatchHandlers.onReadResolveDisappeared(object);
            return object;
        } else {
            return object;
        }
    }

    private Object applyReadResolve(Object objectToResolve, ClassDescriptor descriptor) throws UnmarshalException {
        try {
            return descriptor.serializationMethods().readResolve(objectToResolve);
        } catch (SpecialMethodInvocationException e) {
            throw new UnmarshalException("Cannot apply readResolve()", e);
        }
    }

    private Object applyReadResolveIfSchemaMismatchHandlerAllows(Object object, ClassDescriptor localDescriptor) throws UnmarshalException {
        boolean shouldInvokeReadResolve = schemaMismatchHandlers.onReadResolveAppeared(object);

        if (shouldInvokeReadResolve) {
            return applyReadResolve(object, localDescriptor);
        } else {
            return object;
        }
    }
}
