package com.maciejwalkowiak.reactorsqs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.lifecycle.Startables;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

@SpringBootTest
@ContextConfiguration(initializers = AbstractIntegrationTest.Initializer.class)
abstract class AbstractIntegrationTest {
	private static final Logger logger = LoggerFactory.getLogger(AbstractIntegrationTest.class);

	static class Initializer
			implements ApplicationContextInitializer<ConfigurableApplicationContext> {

		static Aws2LocalstackContainer localstack = new Aws2LocalstackContainer().withServices("sqs").withReuse(true);

		public static Map<String, String> getProperties() {
			localstack.start();

			Map<String, String> properties = new HashMap<>();
			properties.put("cloud.aws.sqs.default-listener.endpoint",
					localstack.getEndpointOverride().toString());
			properties.put("cloud.aws.sqs.default-listener.region",
					localstack.getRegion());

			return properties;
		}

		@Override
		public void initialize(ConfigurableApplicationContext context) {
			ConfigurableEnvironment env = context.getEnvironment();
			env.getPropertySources().addFirst(new MapPropertySource(
					"testcontainers",
					(Map) getProperties()
			));

			logger.info("SQS exposed under {}", localstack.getEndpointOverride());

			// create queues
			SqsClient sqs = SqsClient.builder()
				.endpointOverride(localstack.getEndpointOverride())
				.region(Region.of(localstack.getRegion()))
				.build();
			sqs.createQueue(CreateQueueRequest.builder().queueName("my-new-queue").build());
		}
	}
}
