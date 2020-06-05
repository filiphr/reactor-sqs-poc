package com.maciejwalkowiak.reactorsqs;

import com.maciejwalkowiak.reactorsqs.sqs.SqsProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.net.URI;
import java.time.Duration;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@ContextConfiguration(initializers = AbstractIntegrationTest.Initializer.class)
class ReactorSqsApplicationTests {

	@Autowired
	protected SqsProperties sqsProperties;

	@Autowired
	protected MyListener myListener;

	@Test
	void contextLoads() {

		SqsClient sqs = SqsClient.builder()
			.endpointOverride(sqsProperties.getDefaultListener().getEndpoint())
			.region(sqsProperties.getDefaultListener().getRegion())
			.build();

		String queueUrl = sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName("my-new-queue").build()).queueUrl();

		sqs.sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageBody("test").build());


		Collection<Message> messages = await("receive messages")
			.atMost(Duration.ofSeconds(5))
			.pollInterval(Duration.ofMillis(200))
			.until(() -> myListener.getReceivedMessages(), msgs -> !msgs.isEmpty());

		Message message = messages.iterator().next();
		assertThat(message.body()).isEqualTo("test");

	}

}
