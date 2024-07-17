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

package org.apache.ignite.internal.fileio;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * For {@link AsyncFileIo} testing.
 */
public class AsyncFileIoTest extends AbstractFileIoTest {
    @BeforeEach
    void setUp() {
        fileIoFactory = new AsyncFileIoFactory();
    }

    /** {@inheritDoc} */
    @Override
    protected Class<? extends FileIo> fileIoClass() {
        return AsyncFileIo.class;
    }

    @Test
    @Override
    void testMap() {
        assertThrows(UnsupportedOperationException.class, super::testMap);
    }
}
