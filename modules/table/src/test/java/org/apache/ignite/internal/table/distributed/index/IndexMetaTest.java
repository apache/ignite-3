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

package org.apache.ignite.internal.table.distributed.index;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.util.Map;
import org.junit.jupiter.api.Test;

class IndexMetaTest {
    @Test
    void statusAtReturnsCorrespondingStatus() {
        Map<MetaIndexStatus, MetaIndexStatusChange> statusChanges = Map.of(
                MetaIndexStatus.REGISTERED, new MetaIndexStatusChange(1, 10),
                MetaIndexStatus.BUILDING, new MetaIndexStatusChange(2, 20)
        );
        IndexMeta indexMeta = new IndexMeta(2, 1, 1, 1, "test", MetaIndexStatus.BUILDING, statusChanges);

        assertThat(indexMeta.statusAt(9), is(nullValue()));

        assertThat(indexMeta.statusAt(10), is(MetaIndexStatus.REGISTERED));
        assertThat(indexMeta.statusAt(11), is(MetaIndexStatus.REGISTERED));
        assertThat(indexMeta.statusAt(19), is(MetaIndexStatus.REGISTERED));

        assertThat(indexMeta.statusAt(20), is(MetaIndexStatus.BUILDING));
        assertThat(indexMeta.statusAt(2000), is(MetaIndexStatus.BUILDING));
    }
}
