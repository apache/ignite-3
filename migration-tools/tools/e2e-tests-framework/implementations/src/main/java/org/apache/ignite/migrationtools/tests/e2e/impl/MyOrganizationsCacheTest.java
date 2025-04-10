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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Random;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.ignite.examples.model.Address;
import org.apache.ignite.examples.model.Organization;
import org.apache.ignite.examples.model.OrganizationType;
import org.apache.ignite.migrationtools.tests.e2e.framework.core.ExampleBasedCacheTest;
import org.apache.ignite.table.mapper.Mapper;

/** MyOrganizationsCacheTest. */
@AutoService(ExampleBasedCacheTest.class)
public class MyOrganizationsCacheTest extends VeryBasicAbstractCacheTest<Long, Organization> {
    private static OrganizationType[] FROZEN_ORG_TYPES = new OrganizationType[]{
            OrganizationType.GOVERNMENT, OrganizationType.PRIVATE, OrganizationType.NON_PROFIT };

    public MyOrganizationsCacheTest() {
        super(Long.class, Organization.class);
    }

    @Override
    public String getTableName() {
        return "MyOrganizations";
    }

    @SuppressFBWarnings("DMI_RANDOM_USED_ONLY_ONCE")
    @Override
    public Map.Entry<Long, Organization> supplyExample(int seed) {
        Random r = new Random(100 + seed);

        int port = r.nextInt(100);
        int zip = 1000 + r.nextInt(9000);
        Address addr = new Address("My Address " + port, zip);

        int typeRnd = r.nextInt(3);
        OrganizationType type = FROZEN_ORG_TYPES[typeRnd];

        long timestampRng = r.nextInt(Integer.MAX_VALUE) * 1000L;

        Organization o = new Organization("organization-" + seed, addr, type, new Timestamp(timestampRng));

        try {
            // Inject the id otherwise it will be auto-generated.
            FieldUtils.writeField(o, "id", (long) seed, true);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        return Map.entry((long) seed, o);
    }

    @Override
    protected Mapper<Organization> valMapper() {
        // TODO: Add the other fields if this is supported in the future
        return Mapper.builder(Organization.class)
                .map("id", "ID")
                .map("name", "NAME")
                .build();
    }

    @Override
    protected void assertValueFromIgnite3(Organization actualVal, Organization expected) {
        assertThat(actualVal)
                .extracting(Organization::id, Organization::name, Organization::address, Organization::type, Organization::lastUpdated)
                .containsExactly(expected.id(), expected.name(), null, null, null);
    }

    @Override
    protected void assertResultSet(ResultSet rs, Organization expectedObj) throws SQLException {
        assertThat(rs.getLong("ID")).isEqualTo(expectedObj.id());
        assertThat(rs.getString("NAME")).isEqualTo(expectedObj.name());
    }
}
