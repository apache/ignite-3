/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.tests.e2e.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.auto.service.AutoService;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import org.apache.ignite.migrationtools.tests.e2e.framework.core.ExampleBasedCacheTest;

/** MySimpleMapCacheTest. */
@AutoService(ExampleBasedCacheTest.class)
public class MySimpleMapCacheTest extends VeryBasicAbstractCacheTest<String, Integer> {

    public MySimpleMapCacheTest() {
        super(String.class, Integer.class, "ID");
    }

    @Override
    public String getTableName() {
        return "MySimpleMap";
    }

    @Override
    public Map.Entry<String, Integer> supplyExample(int seed) {
        return Map.entry("MyKey:" + seed, seed);
    }

    @Override
    protected void assertResultSet(ResultSet rs, Integer expectedObj) throws SQLException {
        assertThat(rs.getInt("VAL")).isEqualTo(expectedObj);
    }
}
