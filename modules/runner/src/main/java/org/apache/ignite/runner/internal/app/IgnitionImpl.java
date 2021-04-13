/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.runner.internal.app;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import io.netty.util.internal.StringUtil;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.app.Ignition;
import org.apache.ignite.baseline.internal.BaselineManager;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.configuration.schemas.network.NetworkConfiguration;
import org.apache.ignite.configuration.schemas.network.NetworkView;
import org.apache.ignite.configuration.schemas.runner.LocalConfiguration;
import org.apache.ignite.affinity.internal.AffinityManager;
import org.apache.ignite.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.affinity.ditributed.AffinityManager;
import org.apache.ignite.internal.table.distributed.TableManagerImpl;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.metastorage.internal.MetaStorageManager;
import org.apache.ignite.network.Network;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.scalecube.ScaleCubeMemberResolver;
import org.apache.ignite.network.scalecube.ScaleCubeNetworkClusterFactory;
import org.apache.ignite.raft.internal.Loza;
import org.apache.ignite.runner.internal.storage.DistributedConfigurationStorage;
import org.apache.ignite.runner.internal.storage.LocalConfigurationStorage;
import org.apache.ignite.schema.internal.SchemaManager;
import org.apache.ignite.table.manager.TableManager;
import org.apache.ignite.utils.IgniteProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IgnitionImpl implements Ignition {
    /** */
    private static final String[] BANNER = new String[] {
        "",
        "           #              ___                         __",
        "         ###             /   |   ____   ____ _ _____ / /_   ___",
        "     #  #####           / /| |  / __ \\ / __ `// ___// __ \\ / _ \\",
        "   ###  ######         / ___ | / /_/ // /_/ // /__ / / / // ___/",
        "  #####  #######      /_/  |_|/ .___/ \\__,_/ \\___//_/ /_/ \\___/",
        "  #######  ######            /_/",
        "    ########  ####        ____               _  __           _____",
        "   #  ########  ##       /  _/____ _ ____   (_)/ /_ ___     |__  /",
        "  ####  #######  #       / / / __ `// __ \\ / // __// _ \\     /_ <",
        "   #####  #####        _/ / / /_/ // / / // // /_ / ___/   ___/ /",
        "     ####  ##         /___/ \\__, //_/ /_//_/ \\__/ \\___/   /____/",
        "       ##                  /____/\n"
    };

    /** */
    private static final String VER_KEY = "version";

    /** */
    private static final Logger log = LoggerFactory.getLogger(IgnitionImpl.class);

    @Override public synchronized Ignite start(String jsonStrBootstrapCfg) {
        ackBanner();

        // Vault Component startup.
        VaultManager vaultMgr = new VaultManager();

        boolean cfgBootstrappedFromPds = vaultMgr.bootstrapped();

        List<RootKey<?, ?>> rootKeys = new ArrayList<>(Collections.singletonList(NetworkConfiguration.KEY));

        List<ConfigurationStorage> configurationStorages =
            new ArrayList<>(Collections.singletonList(new LocalConfigurationStorage(vaultMgr)));

        // Bootstrap local configuration manager.
        ConfigurationManager locConfigurationMgr = new ConfigurationManager(rootKeys, configurationStorages);

        if (!cfgBootstrappedFromPds)
            try {
                locConfigurationMgr.bootstrap(jsonStrBootstrapCfg);
            }
            catch (Exception e) {
                log.warn("Unable to parse user specific configuration, default configuration will be used", e);
            }
        else if (jsonStrBootstrapCfg != null)
            log.warn("User specific configuration will be ignored, cause vault was bootstrapped with pds configuration");

        NetworkView netConfigurationView =
            locConfigurationMgr.configurationRegistry().getConfiguration(NetworkConfiguration.KEY).value();

        String localMemberName = locConfigurationMgr.configurationRegistry().getConfiguration(LocalConfiguration.KEY)
            .name().value();

        if (StringUtil.isNullOrEmpty(localMemberName)) {
            localMemberName = "Node: " + netConfigurationView.port();

            String finalName = localMemberName;

            locConfigurationMgr.configurationRegistry().getConfiguration(LocalConfiguration.KEY).change(change ->
                change.changeName(finalName));
        }

        // Network startup.
        Network net = new Network(
            new ScaleCubeNetworkClusterFactory(
                localMemberName,
                netConfigurationView.port(),
                Arrays.asList(netConfigurationView.networkMembersNames()),
                new ScaleCubeMemberResolver())
        );

        // Register component message types.
        // TODO: IGNITE-14088: Uncomment and use real serializer provider
//        Arrays.stream(MetaStorageMessageTypes.values()).forEach(
//            msgTypeInstance -> net.registerMessageMapper(
//                msgTypeInstance.msgType(),
//                new DefaultMessageMapperProvider()
//            )
//        );
//
//        Arrays.stream(RaftMessageTypes.values()).forEach(
//            msgTypeInstance -> net.registerMessageMapper(
//                msgTypeInstance.msgType(),
//                new DefaultMessageMapperProvider()
//            )
//        );

        NetworkCluster netMember = net.start();

        // Raft Component startup.
        Loza raftMgr = new Loza(netMember);

        // MetaStorage Component startup.
        MetaStorageManager metaStorageMgr = new MetaStorageManager(
            netMember,
            raftMgr,
            locConfigurationMgr
        );

        // TODO sanpwc: > Add distributed root keys.
        // TODO sanpwc: > Bootstrap distributed configuration.
        configurationStorages.add(new DistributedConfigurationStorage(metaStorageMgr));

        // Start configuration manager.
        ConfigurationManager configurationMgr = new ConfigurationManager(rootKeys, configurationStorages);

        // Baseline manager startup.
        BaselineManager baselineMgr = new BaselineManager(configurationMgr, metaStorageMgr, netMember);

        // Affinity manager startup.
        AffinityManager affinityMgr = new AffinityManager(configurationMgr, metaStorageMgr, baselineMgr);

        SchemaManager schemaManager = new SchemaManager(configurationMgr);

        // Distributed table manager startup.
        TableManager distributedTblMgr = new TableManagerImpl(
            configurationMgr,
            netMember,
            metaStorageMgr,
            schemaManager
        );

        // TODO sanpwc: Start rest manager.

        // Deploy all resisted watches cause all components are ready and have registered their listeners.
        metaStorageMgr.deployWatches();

        ackSuccessStart();

        return new IgniteImpl(configurationMgr, distributedTblMgr);
    }

    /** */
    private static void ackSuccessStart() {
        log.info("Apache Ignite started successfully!");
    }

    /** */
    private static void ackBanner() {
        String ver = IgniteProperties.get(VER_KEY);

        String banner = Arrays
            .stream(BANNER)
            .collect(Collectors.joining("\n"));

        log.info(banner + '\n' + " ".repeat(22) + "Apache Ignite ver. " + ver + '\n');
    }
}