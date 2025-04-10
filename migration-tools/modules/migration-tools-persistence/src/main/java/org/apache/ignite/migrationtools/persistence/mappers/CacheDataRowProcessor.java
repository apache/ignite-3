/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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

    private static Tuple parseBinaryObject(BinaryObjectImpl obj) {
        var fieldNames = obj.rawType().fieldNames();
        Map<String, Object> fields = new HashMap<>(fieldNames.size());
        for (String fieldName : fieldNames) {
            var val = obj.field(fieldName);
            if (val instanceof BinaryObject) {
                BinaryObject nested = ((BinaryObject) val);
                if (nested.type().isEnum()) {
                    val = nested.enumName();
                } else {
                    val = parseBinaryObject((BinaryObjectImpl) val);
                }
            }
            fields.put(fieldName, val);
        }
        return Tuple.create(fields);
    }
}
