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

import java.util.List;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;

/** CREATE INDEX statement. */
public class CreateSortedIndexParams extends AbstractCreateIndexCommandParams {
    /** Creates parameters builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** Columns collations. */
    private List<CatalogColumnCollation> collations;

    /** Gets columns collations. */
    public List<CatalogColumnCollation> collations() {
        return collations;
    }

    /** Parameters builder. */
    public static class Builder extends AbstractCreateIndexBuilder<CreateSortedIndexParams, Builder> {
        private Builder() {
            super(new CreateSortedIndexParams());
        }

        /**
         * Set columns collations.
         *
         * @param collations Columns collations.
         * @return {@code this}.
         */
        public Builder collations(List<CatalogColumnCollation> collations) {
            params.collations = collations;

            return this;
        }
    }
}
