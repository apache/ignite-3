package org.apache.ignite.utils;

import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.schemas.network.NetworkConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NodeFinder;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;

import java.util.Collections;

public class ClusterServiceTestUtils {

    /**
     * Creates a cluster service.
     */
    public static ClusterService clusterService(String nodeName, int port, NodeFinder nodeFinder, MessageSerializationRegistry msgSerializationRegistry, ClusterServiceFactory clusterServiceFactory) {
        var ctx = new ClusterLocalConfiguration(nodeName, msgSerializationRegistry);

        ConfigurationManager nodeConfigurationMgr = new ConfigurationManager(
            Collections.singleton(NetworkConfiguration.KEY),
            Collections.singleton(new TestConfigurationStorage(ConfigurationType.LOCAL))
        );

        var clusterSvc = clusterServiceFactory.createClusterService(
            ctx,
            nodeConfigurationMgr,
            () -> nodeFinder
        );

        var clusterSvcWrapper = new ClusterService() {
            @Override public TopologyService topologyService() {
                return clusterSvc.topologyService();
            }

            @Override public MessagingService messagingService() {
                return clusterSvc.messagingService();
            }

            @Override public ClusterLocalConfiguration localConfiguration() {
                return clusterSvc.localConfiguration();
            }

            @Override public boolean isStopped() {
                return clusterSvc.isStopped();
            }

            @Override public void start() {
                nodeConfigurationMgr.start();

                nodeConfigurationMgr.configurationRegistry().getConfiguration(NetworkConfiguration.KEY).
                    change(netCfg -> netCfg.changePort(port)).join();

                clusterSvc.start();
            }

            @Override public void stop() {
                clusterSvc.stop();
                nodeConfigurationMgr.stop();
            }
        };

        clusterSvcWrapper.start();

        return clusterSvcWrapper;
    }
}
