/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.persistence;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.platform.commons.support.AnnotationSupport.findAnnotatedFields;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.migrationtools.config.Ignite2ConfigurationUtils;
import org.apache.ignite.migrationtools.tests.clusters.FullSampleCluster;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.ModifierSupport;

/** BasePersistentTestContext. */
public class BasePersistentTestContext implements BeforeEachCallback, AfterEachCallback {

    /** recreateNodeContexes. */
    public static List<MigrationKernalContext> recreateNodeContexes() throws IOException, IgniteCheckedException {
        IgniteConfiguration igniteCfg = Ignite2ConfigurationUtils.loadIgnite2Configuration(
                FullSampleCluster.CLUSTER_CFG_PATH.toFile(), false);

        assumeTrue(Files.exists(FullSampleCluster.TEST_CLUSTER_PATH) && Files.isDirectory(FullSampleCluster.TEST_CLUSTER_PATH),
                "The test-cluster must be recreated before running the tests.\n"
                + "Please read the tools/sample-cluster-generator/README.md for instructions on how to recreate the test-cluster.\n"
                + "After running the tool, the cluster workDir should be placed under resources/sample-clusters/test-cluster");

        var cfg = new IgniteConfiguration()
                .setWorkDirectory(FullSampleCluster.TEST_CLUSTER_PATH.toString())
                .setDataStorageConfiguration(new DataStorageConfiguration());

        List<Ignite2PersistenceTools.NodeFolderDescriptor> nodeCandidates = Ignite2PersistenceTools.nodeFolderCandidates(cfg);
        List<MigrationKernalContext> ret = new ArrayList<>(nodeCandidates.size());
        for (Ignite2PersistenceTools.NodeFolderDescriptor candidate : nodeCandidates) {
            MigrationKernalContext ctx = new MigrationKernalContext(igniteCfg, candidate.subFolderFile(), candidate.consistentId());

            ret.add(ctx);
        }

        return ret;
    }

    private static boolean isCompatible(Field field) {
        // Should raise an exception if not the correct type.
        return Arrays.stream(field.getAnnotation(ExtendWith.class).value())
                .anyMatch(klass -> klass == BasePersistentTestContext.class) && ModifierSupport.isNotStatic(field);
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        var klass = context.getRequiredTestClass();
        var instance = context.getRequiredTestInstance();
        for (var field : findAnnotatedFields(klass, ExtendWith.class, BasePersistentTestContext::isCompatible)) {
            var nodeContexes = recreateNodeContexes();
            FieldUtils.writeField(field, instance, nodeContexes, true);
        }
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        var klass = context.getRequiredTestClass();
        var instance = context.getRequiredTestInstance();
        for (var field : findAnnotatedFields(klass, ExtendWith.class, BasePersistentTestContext::isCompatible)) {
            var nodeContexts = (List<MigrationKernalContext>) FieldUtils.readField(field, instance, true);
            for (MigrationKernalContext ctx : nodeContexts) {
                ctx.stop();
            }
        }
    }
}
