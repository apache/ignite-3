package org.apache.ignite.internal.distributionzones.configuration;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ConfigurationExtension.class)
public class DistributionZonesHighAvailabilityConfigurationTest extends BaseIgniteAbstractTest {
    public static final String PARTITION_DISTRIBUTION_RESET_SCALE_DOWN = "partitionDistributionResetScaleDown";

    public static final long RESET_SCALE_DOWN_DEFAULT_VALUE = 0;

    @Test
    void testEmptySystemProperties(@InjectConfiguration SystemDistributedConfiguration systemConfig) {
        var config = new DistributionZonesHighAvailabilityConfiguration(systemConfig);
        config.startAndInit();

        assertEquals(RESET_SCALE_DOWN_DEFAULT_VALUE, config.partitionDistributionResetScaleDown());
    }

    @Test
    void testValidSystemPropertiesOnStart(
            @InjectConfiguration("mock.properties = {"
                    + PARTITION_DISTRIBUTION_RESET_SCALE_DOWN + ".propertyValue = \"5\"}")
            SystemDistributedConfiguration systemConfig
    ) {
        var config = new DistributionZonesHighAvailabilityConfiguration(systemConfig);
        config.startAndInit();

        assertEquals(5, config.partitionDistributionResetScaleDown());
    }

    @Test
    void testValidSystemPropertiesOnChange(@InjectConfiguration SystemDistributedConfiguration systemConfig) {
        var config = new DistributionZonesHighAvailabilityConfiguration(systemConfig);
        config.startAndInit();

        changeSystemConfig(systemConfig, "10");

        assertEquals(10, config.partitionDistributionResetScaleDown());
    }

    private static void changeSystemConfig(
            SystemDistributedConfiguration systemConfig,
            String partitionDistributionResetScaleDown
    ) {
        CompletableFuture<Void> changeFuture = systemConfig.change(c0 -> c0.changeProperties()
                .create(PARTITION_DISTRIBUTION_RESET_SCALE_DOWN, c1 -> c1.changePropertyValue(partitionDistributionResetScaleDown))
        );

        assertThat(changeFuture, willCompleteSuccessfully());
    }
}
