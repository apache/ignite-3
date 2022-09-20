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

package org.apache.ignite.internal.pagememory;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;

/**
 * Extension (for tests) of {@link PageIoRegistry} to be able to add/replace {@link IoVersions} not through {@link #loadFromServiceLoader}.
 */
public class TestPageIoRegistry extends PageIoRegistry {
    private boolean loadFromServiceLoaderCalled;

    @Override
    public void loadFromServiceLoader() {
        loadFromServiceLoaderCalled = true;

        super.loadFromServiceLoader();
    }

    /**
     * Loads or replaces {@link IoVersions versions}.
     *
     * <p>NOTE: {@link #loadFromServiceLoader} must be called first.
     *
     * @param versions IO versions.
     * @throws IllegalStateException If there's an invalid page type.
     */
    public void load(IoVersions<?>... versions) {
        assertTrue(loadFromServiceLoaderCalled, "loadFromServiceLoader must be called first");

        for (IoVersions<?> ios : versions) {
            if (ios.getType() == 0) {
                throw new IllegalStateException("Type 0 is reserved and can't be used: " + ios);
            }

            ioVersions[ios.getType()] = ios;
        }
    }
}
