/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.IdIndexedDescriptors;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link IdIndexedDescriptors} that queries two other {@link IdIndexedDescriptors} instances
 * one of which has a priority.
 */
public class MergingIdIndexedDescriptors implements IdIndexedDescriptors {
    private final IdIndexedDescriptors firstDelegate;
    private final IdIndexedDescriptors secondDelegate;

    public MergingIdIndexedDescriptors(IdIndexedDescriptors firstDelegate, IdIndexedDescriptors secondDelegate) {
        this.firstDelegate = firstDelegate;
        this.secondDelegate = secondDelegate;
    }

    /** {@inheritDoc} */
    @Override
    @Nullable
    public ClassDescriptor getDescriptor(int descriptorId) {
        if (firstDelegate.hasDescriptor(descriptorId)) {
            return firstDelegate.getDescriptor(descriptorId);
        }

        return secondDelegate.getDescriptor(descriptorId);
    }
}
