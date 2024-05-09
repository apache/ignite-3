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

package org.apache.ignite.internal.storage.index;

import static org.apache.ignite.lang.ErrorGroups.Storage.INDEX_NOT_BUILT_ERR;

import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.storage.StorageException;

/** Exception occurring while reading from an index that has not yet been built. */
public class IndexNotBuiltException extends StorageException {
    private static final long serialVersionUID = 7512376065062977603L;

    /**
     * Constructor.
     *
     * @param messagePattern Error message pattern.
     * @param params Error message params.
     * @see IgniteStringFormatter#format(String, Object...)
     */
    public IndexNotBuiltException(String messagePattern, Object... params) {
        super(INDEX_NOT_BUILT_ERR, messagePattern, params);
    }
}
