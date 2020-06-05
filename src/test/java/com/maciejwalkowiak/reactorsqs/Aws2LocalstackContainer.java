package com.maciejwalkowiak.reactorsqs;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.TestcontainersConfiguration;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Filip Hrisafov
 */
public class Aws2LocalstackContainer extends GenericContainer<Aws2LocalstackContainer> {
    // Testcontainers does not support AWS SDK 2 yet. Therefore use a custom container

    public static final String VERSION = "0.11.2";

    private static final String HOSTNAME_EXTERNAL_ENV_VAR = "HOSTNAME_EXTERNAL";

    private static final int DEFAULT_PORT = 4566;

    private final List<String> services = new ArrayList<>();

    public Aws2LocalstackContainer() {
        this(VERSION);
    }

    public Aws2LocalstackContainer(String version) {
        super(TestcontainersConfiguration.getInstance().getLocalStackImage() + ":" + version);

        withFileSystemBind("//var/run/docker.sock", "/var/run/docker.sock");
        waitingFor(Wait.forLogMessage(".*Ready\\.\n", 1));
    }

    @Override
    protected void configure() {
        super.configure();

        withEnv("SERVICES", String.join(",", services));

        String hostnameExternalReason;
        if (getEnvMap().containsKey(HOSTNAME_EXTERNAL_ENV_VAR)) {
            // do nothing
            hostnameExternalReason = "explicitly as environment variable";
        } else if (getNetwork() != null && getNetworkAliases() != null && getNetworkAliases().size() >= 1) {
            withEnv(HOSTNAME_EXTERNAL_ENV_VAR, getNetworkAliases().get(getNetworkAliases().size() - 1));  // use the last network alias set
            hostnameExternalReason = "to match last network alias on container with non-default network";
        } else {
            withEnv(HOSTNAME_EXTERNAL_ENV_VAR, getHost());
            hostnameExternalReason = "to match host-routable address for container";
        }
        logger().info("{} environment variable set to {} ({})", HOSTNAME_EXTERNAL_ENV_VAR, getEnvMap().get(HOSTNAME_EXTERNAL_ENV_VAR), hostnameExternalReason);

        addExposedPort(DEFAULT_PORT);
    }

    public Aws2LocalstackContainer withServices(String... services) {
        this.services.addAll(Arrays.asList(services));
        return self();
    }

    public URI getEndpointOverride() {
        try {
            return new URI("http://" +
                getContainerIpAddress() +
                ":" +
                getMappedPort(DEFAULT_PORT));
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Cannot obtain endpoint URL", e);
        }
    }

    public String getRegion() {
        return "us-east-1";
    }

}