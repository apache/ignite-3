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

package org.apache.ignite.internal.sql.engine.metadata;

import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

/**
 * See {@link RelMetadataQuery}.
 */
public class RelMetadataQueryEx extends RelMetadataQuery {
    /**
     * Factory method.
     *
     * @return return Metadata query instance.
     */
    public static RelMetadataQueryEx create() {
        return create(IgniteMetadata.METADATA_PROVIDER);
    }

    /**
     * Factory method.
     *
     * @return return Metadata query instance.
     */
    public static RelMetadataQueryEx create(RelMetadataProvider metadataProvider) {
        THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(metadataProvider));
        try {
            return new RelMetadataQueryEx();
        } finally {
            THREAD_PROVIDERS.remove();
        }
    }
}
