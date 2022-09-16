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

import static org.apache.ignite.internal.util.IgniteUtils.readableSize;

import java.io.IOException;

/**
 * Utility class for page stores.
 */
class PageStoreUtils {
    /**
     * Checks the page sizes in bytes of the file.
     *
     * @param expPageSize Expected page size in bytes.
     * @param actPageSize Actual page size in bytes.
     * @throws IOException If the page sizes in bytes do not match.
     */
    static void checkFilePageSize(int expPageSize, int actPageSize) throws IOException {
        if (expPageSize != actPageSize) {
            throw new IOException(String.format(
                    "Invalid file pageSize [expected=%s, actual=%s]",
                    readableSize(expPageSize, false),
                    readableSize(actPageSize, false)
            ));
        }
    }

    /**
     * Checks the versions of the file.
     *
     * @param expVersion Expected version.
     * @param actVersion Actual version.
     * @throws IOException If the versions does not match.
     */
    static void checkFileVersion(int expVersion, int actVersion) throws IOException {
        if (expVersion != actVersion) {
            throw new IOException(String.format(
                    "Invalid file version [expected=%s, actual=%s]",
                    expVersion,
                    actVersion
            ));
        }
    }
}
