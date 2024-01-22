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

package org.apache.ignite.internal.catalog;

import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;

/** Exception that occurs on an incorrect attempt to change the {@link CatalogIndexDescriptor#status()}. */
public class ChangeIndexStatusValidationException extends CatalogValidationException {
    private static final long serialVersionUID = 7037807559118991379L;

    /**
     * Constructor.
     *
     * @param indexId Index ID.
     * @param current Current index status.
     * @param target Status of the index to which wanted to change.
     * @param expectedCurrent Expected current index status.
     */
    public ChangeIndexStatusValidationException(
            int indexId,
            CatalogIndexStatus current,
            CatalogIndexStatus target,
            CatalogIndexStatus expectedCurrent
    ) {
        super(
                "It is impossible to change the index status: [indexId={}, from={}, to={}, expectedCurrent={}]",
                indexId, current, target, expectedCurrent
        );
    }
}
