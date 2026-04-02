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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class OldestFirstCompactionStrategyTest extends AbstractCompactionStrategyTest {
    @Override
    protected SegmentFileCompactionStrategy createStrategy(Path segmentFilesDir, IndexFileManager indexFileManager) {
        return new OldestFirstCompactionStrategy(segmentFilesDir, indexFileManager);
    }

    @Test
    void ordersCandidatesByNaturalOrder() throws IOException {
        FileProperties oldest = new FileProperties(1, 0);
        FileProperties newestOrdinal = new FileProperties(3, 0);
        FileProperties sameOrdinalNewerGeneration = new FileProperties(1, 1);

        createSegmentFile(newestOrdinal);
        createSegmentFile(sameOrdinalNewerGeneration);
        createSegmentFile(oldest);

        createIndexFile(newestOrdinal);
        createIndexFile(sameOrdinalNewerGeneration);
        createIndexFile(oldest);

        assertThat(selectedCandidates(), contains(oldest, sameOrdinalNewerGeneration, newestOrdinal));
    }
}
