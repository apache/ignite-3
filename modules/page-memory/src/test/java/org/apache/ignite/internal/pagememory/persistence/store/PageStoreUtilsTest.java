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

package org.apache.ignite.internal.pagememory.persistence.store;

import static org.apache.ignite.internal.pagememory.persistence.store.PageStoreUtils.checkFilePageSize;
import static org.apache.ignite.internal.pagememory.persistence.store.PageStoreUtils.checkFileVersion;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.junit.jupiter.api.Test;

/**
 * For {@link PageStoreUtils} testing.
 */
public class PageStoreUtilsTest {
    @Test
    void testCheckFilePageSize() {
        assertDoesNotThrow(() -> checkFilePageSize(100, 100));

        Exception exception = assertThrows(IOException.class, () -> checkFilePageSize(100, 500));

        assertThat(exception.getMessage(), startsWith("Invalid file pageSize"));
    }

    @Test
    void testCheckFileVersion() {
        assertDoesNotThrow(() -> checkFileVersion(1, 1));

        Exception exception = assertThrows(IOException.class, () -> checkFileVersion(1, 2));

        assertThat(exception.getMessage(), startsWith("Invalid file version"));
    }
}
