/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.persistence.mappers;

import java.util.Collection;
import java.util.Collections;

/** RecordAndTableSchemaMismatchException. */
public class RecordAndTableSchemaMismatchException extends Exception {
    private final Collection<String> missingColumnsInRecord;
    private final Collection<String> additionalColumnsInRecord;

    /** Constructor. */
    public RecordAndTableSchemaMismatchException(Collection<String> missingColumnsInRecord, Collection<String> additionalColumnsInRecord) {
        super();
        this.missingColumnsInRecord = missingColumnsInRecord;
        this.additionalColumnsInRecord = additionalColumnsInRecord;
    }

    @Override
    public String getMessage() {
        return this.toString();
    }

    public Collection<String> additionalColumnsInRecord() {
        return Collections.unmodifiableCollection(additionalColumnsInRecord);
    }

    public Collection<String> missingColumnsInRecord() {
        return Collections.unmodifiableCollection(missingColumnsInRecord);
    }

    @Override
    public String toString() {
        return "RecordAndTableSchemaMismatchException{" + "missingColumnsInRecord=" + missingColumnsInRecord
                + ", additionalColumnsInRecord=" + additionalColumnsInRecord + '}';
    }
}
