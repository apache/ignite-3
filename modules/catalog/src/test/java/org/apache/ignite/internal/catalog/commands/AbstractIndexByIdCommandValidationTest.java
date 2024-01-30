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

package org.apache.ignite.internal.catalog.commands;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;

import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.IndexNotFoundValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.junit.jupiter.api.Test;

/** Abstract test for commands that do something with an index identifying it by ID. */
@SuppressWarnings("ThrowableNotThrown")
public abstract class AbstractIndexByIdCommandValidationTest extends AbstractCommandValidationTest {
    /**
     * Returns a new command that changes the {@link CatalogIndexDescriptor#status() index status}.
     *
     * @param indexId Index ID.
     */
    abstract CatalogCommand createCommand(int indexId);

    @Test
    void exceptionIsThrownIfIndexWithGivenIdNotFound() {
        Catalog catalog = emptyCatalog();

        CatalogCommand command = createCommand(1);

        assertThrowsWithCause(
                () -> command.get(catalog),
                IndexNotFoundValidationException.class,
                "Index with ID '1' not found"
        );
    }
}
