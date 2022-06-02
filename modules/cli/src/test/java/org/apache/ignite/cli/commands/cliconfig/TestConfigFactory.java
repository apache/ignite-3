package org.apache.ignite.cli.commands.cliconfig;

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Replaces;
import jakarta.inject.Singleton;
import java.io.File;
import java.io.IOException;
import org.apache.ignite.cli.config.Config;
import org.apache.ignite.cli.config.ConfigFactory;

/**
 * Test factory for {@link Config}.
 */
@Factory
@Replaces(factory = ConfigFactory.class)
public class TestConfigFactory {

    /**
     * Creates a {@link Config} with some defaults for testing.
     *
     * @return {@link Config}
     * @throws IOException in case temp file couldn't be created
     */
    @Singleton
    public Config createConfig() throws IOException {
        File tempFile = File.createTempFile("cli", null);
        tempFile.deleteOnExit();
        Config config = new Config(tempFile);
        config.setProperty("ignite.cluster-url", "test_cluster_url");
        config.setProperty("ignite.jdbc-url", "test_jdbc_url");
        return config;
    }
}
