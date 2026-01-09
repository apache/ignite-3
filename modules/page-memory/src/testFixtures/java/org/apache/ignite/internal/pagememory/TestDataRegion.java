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

/**
 * A test implementation of the {@link DataRegion} interface that returns the associated {@link PageMemory} instance.
 * The region size is fixed at {@code 0} for testing purposes.
 */
public class TestDataRegion<T extends PageMemory> implements DataRegion<T> {
    private final T pageMemory;

    public TestDataRegion(T pageMemory) {
        this.pageMemory = pageMemory;
    }

    @Override
    public T pageMemory() {
        return pageMemory;
    }

    @Override
    public long regionSize() {
        return 0;
    }
}
