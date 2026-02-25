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

package org.apache.ignite.migrationtools.persistence.mappers;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.migrationtools.persistence.utils.pubsub.BasicProcessor;
import org.apache.ignite3.table.Tuple;

/** Processes {@link CacheDataRow} into Java Object entries. */
public class CacheDataRowProcessor extends BasicProcessor<CacheDataRow, Map.Entry<Object, Object>> {

    private final CacheObjectContext cacheObjectCtx;

    public CacheDataRowProcessor(CacheObjectContext ctx) {
        cacheObjectCtx = ctx;
    }

    @Override
    public void onNext(CacheDataRow row) {
        var key = row.key();
        Object keyVal = (key instanceof BinaryObjectImpl) ? key : key.value(cacheObjectCtx, false);

        var val = row.value();
        Object valVal = (val instanceof BinaryObjectImpl) ? val : val.value(cacheObjectCtx, false);

        // TODO: I prefer to have some typings.
        subscriber.onNext(Map.entry(keyVal, valVal));
    }
}
