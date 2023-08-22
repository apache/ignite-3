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

package org.apache.ignite.otel.ext;

import com.google.auto.service.AutoService;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizer;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizerProvider;
import io.opentelemetry.sdk.autoconfigure.spi.ConfigProperties;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.OperationsException;
import org.apache.ignite.otel.ext.sampler.DynamicRatioSampler;

/**
 * This is one of the main entry points for Instrumentation Agent's customizations. It allows configuring the
 * {@link AutoConfigurationCustomizer}. See the {@link #customize(AutoConfigurationCustomizer)} method below.
 *
 * <p>Also see https://github.com/open-telemetry/opentelemetry-java/issues/2022
 *
 * @see AutoConfigurationCustomizerProvider
 */
@AutoService(AutoConfigurationCustomizerProvider.class)
public class DemoAutoConfigurationCustomizerProvider implements AutoConfigurationCustomizerProvider {
    /** {@inheritDoc} */
    @Override
    public void customize(AutoConfigurationCustomizer autoConfiguration) {
        autoConfiguration.addPropertiesCustomizer(DemoAutoConfigurationCustomizerProvider::customizeIncludedMethods)
                .addPropertiesCustomizer(DemoAutoConfigurationCustomizerProvider::customizeIgniteExecutors)
                .addSamplerCustomizer(DemoAutoConfigurationCustomizerProvider::customizeSampler);
    }

    private static Map<String, String> customizeIgniteExecutors(ConfigProperties configProperties) {
        return Map.of("otel.instrumentation.executors.include", "org.apache.ignite.internal.thread.StripedThreadPoolExecutor, "
                + "org.apache.ignite.internal.thread.StripedScheduledThreadPoolExecutor");

    }

    private static Map<String, String> customizeIncludedMethods(ConfigProperties configProperties) {
        String existed = configProperties.getString("otel.instrumentation.methods.include");
        Set<String> methods = Set.of(
                "org.apache.ignite.client.handler.ClientInboundMessageHandler"
                        + "[channelRead,handshake,processOperation,write,writeError,writeErrorCore]",
                "org.apache.ignite.client.handler.JdbcQueryEventHandlerImpl"
                        + "[queryAsync,batchAsync,batchPrepStatementAsync,finishTxAsync,createJdbcResult]",
                "org.apache.ignite.internal.sql.engine.schema.CatalogSqlSchemaManager"
                        + "[schema]",
                "org.apache.ignite.internal.tx.impl.IgniteAbstractTransactionImpl"
                        + "[commit,commitAsync,rollback,rollbackAsync]",
                "org.apache.ignite.internal.tx.impl.ReadOnlyTransactionImpl"
                        + "[commit,commitAsync,rollback,rollbackAsync]",
                "org.apache.ignite.internal.tx.impl.ReadWriteTransactionImpl"
                        + "[commit,commitAsync,rollback,rollbackAsync]",
                "org.apache.ignite.internal.table.distributed.storage.InternalTableImpl"
                        + "[enlist,enlistInTx,enlistCursorInTx,enlistWithRetry,postEnlist,get,getAll,upsert,upsertAll,getAndUpsert,insert,"
                        + "insertAll,replace,getAndReplace,delete,deleteExact,getAndDelete,deleteAll,deleteAllExact]",
                "org.apache.ignite.internal.table.KeyValueBinaryViewImpl"
                        + "[get,getAsync,getNullable,getNullableAsync,getOrDefault,getOrDefaultAsync,getAll,getAllAsync,"
                        + "contains,containsAsync,put,putAsync,putAll,putAllAsync,getAndPut,getAndPutAsync,"
                        + "getNullableAndPut,getNullableAndPutAsync,putIfAbsent,putIfAbsentAsync,remove,removeAsync,"
                        + "removeAll,removeAllAsync,getAndRemove,getAndRemoveAsync,getNullableAndRemove,getNullableAndRemoveAsync,"
                        + "replace,replaceAsync,getAndReplace,getAndReplaceAsync,getNullableAndReplace,getNullableAndReplaceAsync,"
                        + "streamData]",
                "org.apache.ignite.internal.table.KeyValueViewImpl"
                        + "[get,getAsync,getNullable,getNullableAsync,getOrDefault,getOrDefaultAsync,getAll,getAllAsync,"
                        + "contains,containsAsync,put,putAsync,putAll,putAllAsync,getAndPut,getAndPutAsync,"
                        + "getNullableAndPut,getNullableAndPutAsync,putIfAbsent,putIfAbsentAsync,remove,removeAsync,"
                        + "removeAll,removeAllAsync,getAndRemove,getAndRemoveAsync,getNullableAndRemove,getNullableAndRemoveAsync,"
                        + "replace,replaceAsync,getAndReplace,getAndReplaceAsync,getNullableAndReplace,getNullableAndReplaceAsync,"
                        + "streamData]",
                "org.apache.ignite.internal.sql.engine.sql.ParserServiceImpl"
                        + "[parse]",
                "org.apache.ignite.internal.sql.engine.prepare.PrepareServiceImpl"
                        + "[prepareAsync]",
                "org.apache.ignite.internal.table.RecordBinaryViewImpl"
                        + "[get,getAsync,getAll,getAllAsync,upsert,upsertAsync,upsertAll,upsertAllAsync,getAndUpsert,getAndUpsertAsync,"
                        + "insert,insertAsync,insertAll,insertAllAsync,replace,replaceAsync,getAndReplace,getAndReplaceAsync,"
                        + "delete,deleteAsync,deleteExact,deleteExactAsync,getAndDelete,getAndDeleteAsync,deleteAll,deleteAllAsync,"
                        + "deleteAllExact,deleteAllExactAsync,streamData,marshal,wrap]",
                "org.apache.ignite.internal.table.RecordViewImpl"
                        + "[get,getAsync,getAll,getAllAsync,upsert,upsertAsync,upsertAll,upsertAllAsync,getAndUpsert,getAndUpsertAsync,"
                        + "insert,insertAsync,insertAll,insertAllAsync,replace,replaceAsync,getAndReplace,getAndReplaceAsync,"
                        + "delete,deleteAsync,deleteExact,deleteExactAsync,getAndDelete,getAndDeleteAsync,deleteAll,deleteAllAsync,"
                        + "deleteAllExact,deleteAllExactAsync,streamData]",
                "org.apache.ignite.internal.replicator.ReplicaService"
                        + "[sendToReplica]",
                "org.apache.ignite.internal.sql.engine.SqlQueryProcessor"
                        + "[createSession,closeSession,querySingleAsync,querySingle0]",
                "org.apache.ignite.internal.sql.engine.schema.SqlSchemaManagerImpl"
                        + "[schema]",
                "org.apache.ignite.internal.tx.impl.TxManagerImpl"
                        + "[begin,changeState,finish,cleanup]",
                "org.apache.ignite.internal.sql.engine.exec.ExecutionServiceImpl"
                        + "[executePlan,executeQuery,executeExplain,executeDdl]",
                "org.apache.ignite.internal.table.distributed.TableManager"
                        + "[alterTableAsync,createTableAsync,createTableAsyncInternal,dropTableAsync]",
                "org.apache.ignite.internal.sql.engine.exec.ddl.DdlCommandHandler"
                        + "[handle,handleCreateTable,handleDropTable]",
                "org.apache.ignite.internal.catalog.CatalogManagerImpl"
                        + "[createTable,dropTable,addColumn,dropColumn,alterColumn,createIndex,dropIndex,createZone,dropZone,alterZone,"
                        + "renameZone,saveUpdate,saveUpdateAndWaitForActivation]",
                "org.apache.ignite.internal.catalog.CatalogManagerImpl"
                        + "[saveUpdateAndWaitForActivation,saveUpdate]",
                "org.apache.ignite.internal.distributionzones.DistributionZoneManager"
                        + "[zoneIdAsyncInternal,directZoneIdInternal]",
                "org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutorImpl"
                        + "[execute,submit]"
        );
        String joined = String.join(";", methods);
        Map<String, String> properties = new HashMap<>();
        properties.put("otel.instrumentation.methods.include", existed == null ? joined : existed + ";" + joined);
        return properties;
    }

    private static Sampler customizeSampler(Sampler sampler, ConfigProperties configProperties) {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            DynamicRatioSampler dynamicRatioSampler = new DynamicRatioSampler();
            mbs.registerMBean(dynamicRatioSampler, new ObjectName("org.apache.ignite.otel.ext:type=IgniteDynamicRatioSampler"));
            return dynamicRatioSampler;
        } catch (OperationsException | MBeanRegistrationException e) {
            throw new RuntimeException(e);
        }
    }
}