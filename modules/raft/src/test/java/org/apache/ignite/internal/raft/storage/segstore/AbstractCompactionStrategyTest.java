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

package org.apache.ignite.internal.raft.storage.segstore;

import static org.apache.ignite.internal.raft.storage.segstore.IndexFileManager.indexFileName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
abstract class AbstractCompactionStrategyTest extends IgniteAbstractTest {
    private Path segmentFilesDir;

    private Path indexFilesDir;

    @Mock
    IndexFileManager indexFileManager;

    private SegmentFileCompactionStrategy strategy;

    @BeforeEach
    void setUp() throws IOException {
        segmentFilesDir = Files.createDirectories(workDir.resolve("segment"));
        indexFilesDir = Files.createDirectories(workDir.resolve("index"));

        when(indexFileManager.indexFilePath(any())).thenAnswer(inv -> indexFilesDir.resolve(indexFileName(inv.getArgument(0))));

        strategy = createStrategy(segmentFilesDir, indexFileManager);
    }

    @Test
    void selectsOnlySegmentFilesWithExistingIndexAndSkipsTmp() throws IOException {
        FileProperties missingIndex = new FileProperties(0);
        FileProperties presentIndex = new FileProperties(1);

        createSegmentFile(missingIndex);
        createSegmentFile(presentIndex);

        Files.createFile(segmentFilesDir.resolve(SegmentFile.fileName(new FileProperties(2)) + ".tmp"));

        createIndexFile(presentIndex);

        assertThat(selectedCandidates(), contains(presentIndex));
    }

    abstract SegmentFileCompactionStrategy createStrategy(Path segmentFilesDir, IndexFileManager indexFileManager);

    List<FileProperties> selectedCandidates() throws IOException {
        try (Stream<FileProperties> candidates = strategy.selectCandidates()) {
            return candidates.collect(Collectors.toList());
        }
    }

    void createSegmentFile(FileProperties fileProperties) throws IOException {
        Files.createFile(segmentFilesDir.resolve(SegmentFile.fileName(fileProperties)));
    }

    void createIndexFile(FileProperties fileProperties) throws IOException {
        Files.createFile(indexFilesDir.resolve(indexFileName(fileProperties)));
    }
}
