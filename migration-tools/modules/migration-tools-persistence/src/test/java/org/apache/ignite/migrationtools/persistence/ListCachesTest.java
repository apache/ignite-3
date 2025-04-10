/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.migrationtools.tests.clusters.FullSampleCluster;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.extension.ExtendWith;

/** ListCachesTest. */
@DisabledIfSystemProperty(
        named = "tests.containers.support",
        matches = "false",
        disabledReason = "Lack of support in TeamCity for testcontainers")
@ExtendWith(FullSampleCluster.class)
public class ListCachesTest {

    @ExtendWith(BasePersistentTestContext.class)
    private List<MigrationKernalContext> nodeContexts;

    @BeforeEach
    void startContexts() throws IgniteCheckedException {
        for (MigrationKernalContext ctx : nodeContexts) {
            ctx.start();
        }
        // Stop is done automatically.
    }

    @Test
    void checkCachesAreInTheSampleCluster() throws IgniteCheckedException {
        var foundCaches = Ignite2PersistentCacheTools.persistentCaches(nodeContexts);
        assertThat(foundCaches).hasSize(8);
        // Value extract from the seeddata-container.log
    }

}
