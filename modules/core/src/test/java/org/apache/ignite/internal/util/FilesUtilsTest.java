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

package org.apache.ignite.internal.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class FilesUtilsTest {
    @Test
    void sortByNames() {
        File file1 = new File("C.txt");
        File file2 = new File("A.txt");
        File file3 = new File("B.txt");
        List<File> files = Arrays.asList(file1, file2, file3);

        List<File> sortedFiles = FilesUtils.sortByNames(files);

        assertEquals("A.txt", sortedFiles.get(0).getName());
        assertEquals("B.txt", sortedFiles.get(1).getName());
        assertEquals("C.txt", sortedFiles.get(2).getName());
    }

    @Test
    void sortByNamesWithEmptyList() {
        List<File> files = Collections.emptyList();
        List<File> sortedFiles = FilesUtils.sortByNames(files);
        assertTrue(sortedFiles.isEmpty());
    }
}
