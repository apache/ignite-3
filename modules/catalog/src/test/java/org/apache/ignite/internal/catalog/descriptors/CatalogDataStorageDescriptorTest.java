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

package org.apache.ignite.internal.catalog.descriptors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

import org.junit.jupiter.api.Test;

class CatalogDataStorageDescriptorTest {
    @Test
    void toStringContainsTypeAndFields() {
        var descriptor = new CatalogDataStorageDescriptor("the-engine", "the-region");

        String toString = descriptor.toString();

        assertThat(toString, startsWith("CatalogDataStorageDescriptor ["));
        assertThat(toString, containsString("engine=the-engine"));
        assertThat(toString, containsString("dataRegion=the-region"));
    }
}
