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

package org.apache.ignite.internal.pagememory;

import static org.apache.ignite.internal.pagememory.TestPageIOModule.TEST_PAGE_TYPE;
import static org.apache.ignite.internal.pagememory.TestPageIOModule.TEST_PAGE_VER;
import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.pagememory.io.PageIO;
import org.apache.ignite.internal.pagememory.io.PageIORegistry;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link PageIORegistry} methods.
 */
public class PageIORegistryTest {
    private PageIORegistry ioRegistry = new PageIORegistry();

    @Test
    void testResolve() throws Exception {
        assertThrows(IgniteInternalCheckedException.class, () -> ioRegistry.resolve(TEST_PAGE_TYPE, TEST_PAGE_VER));

        // Load all PageIOModule-s from the classpath.
        ioRegistry.loadFromServiceLoader();

        // Test base resolve method.
        PageIO pageIO = ioRegistry.resolve(TEST_PAGE_TYPE, TEST_PAGE_VER);

        assertNotNull(pageIO);
        assertEquals(TEST_PAGE_TYPE, pageIO.getType());
        assertEquals(TEST_PAGE_VER, pageIO.getVersion());

        ByteBuffer pageBuffer = ByteBuffer.allocateDirect(4);
        pageBuffer.order(GridUnsafe.NATIVE_BYTE_ORDER);

        pageBuffer.putShort(PageIO.TYPE_OFF, (short) TEST_PAGE_TYPE);
        pageBuffer.putShort(PageIO.VER_OFF, (short) TEST_PAGE_VER);

        // Test resolve from a pointer.
        assertEquals(pageIO, ioRegistry.resolve(bufferAddress(pageBuffer)));

        // Test resolve from ByteBuffer.
        assertEquals(pageIO, ioRegistry.resolve(pageBuffer));
    }
}
