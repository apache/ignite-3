/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.tests.models;

import java.util.Objects;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;

/** ComplexKeyIntStr. */
public class ComplexKeyIntStr {
    // TODO: Checkout to change fields casing - https://issues.apache.org/jira/browse/IGNITE-6785
    private int id;

    @AffinityKeyMapped
    private String affinityStr;

    public ComplexKeyIntStr(int id, String affinityStr) {
        this.id = id;
        this.affinityStr = affinityStr;
    }

    public int getId() {
        return id;
    }

    public String getAffinityStr() {
        return affinityStr;
    }

    @Override public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ComplexKeyIntStr str = (ComplexKeyIntStr) o;
        return id == str.id && Objects.equals(affinityStr, str.affinityStr);
    }

    @Override public int hashCode() {
        return Objects.hash(id, affinityStr);
    }
}
