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

package org.apache.ignite.internal.pagememory.tree;

import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.freelist.FreeListImpl;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;

/**
 * An abstract class for testing {@link BplusTree} with {@link ReuseList} using different implementations of {@link PageMemory}.
 */
public abstract class AbstractBplusTreeReusePageMemoryTest extends AbstractBplusTreePageMemoryTest {
    /** {@inheritDoc} */
    @Override
    protected ReuseList createReuseList(
            int grpId,
            int partId,
            PageMemory pageMem,
            long rootId,
            boolean initNew
    ) throws IgniteInternalCheckedException {
        return new FreeListImpl(
                "test",
                grpId,
                partId,
                pageMem,
                rootId,
                initNew,
                null
        );
    }
}
