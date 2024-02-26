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

package org.apache.ignite.internal.table.distributed.replicator;

import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.util.Cursor;

/**
 * A resource to be used in {@link RemotelyTriggeredResourceRegistry}.
 */
public class CursorResource implements ManuallyCloseable {

    private final Cursor<?> cursor;

    public CursorResource(Cursor<?> cursor) {
        this.cursor = cursor;
    }

    public <T extends Cursor<?>> T cursor() {
        return (T) cursor;
    }

    @Override
    public void close() throws Exception {
        cursor.close();
    }
}
