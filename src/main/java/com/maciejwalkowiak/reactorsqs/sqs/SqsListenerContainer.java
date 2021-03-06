package com.maciejwalkowiak.reactorsqs.sqs;

import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.maciejwalkowiak.reactorsqs.sqs.SqsProperties.ListenerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import org.springframework.beans.factory.DisposableBean;

public class SqsListenerContainer implements DisposableBean {
	private static final Logger logger = LoggerFactory.getLogger(SqsListenerContainer.class);
	private final String listenerName;
	private final ListenerProperties listenerProperties;
	private final SqsAsyncClient sqs;
	private final Object target;
	private final Method targetMethod;

	private Disposable disposable;
	private Scheduler scheduler;

	private String queueUrl;

	public SqsListenerContainer(String listenerName, ListenerProperties listenerProperties, SqsAsyncClient sqs, Object target, Method targetMethod) {
		this.listenerName = listenerName;
		this.listenerProperties = listenerProperties;
		this.sqs = sqs;
		this.target = target;
		this.targetMethod = targetMethod;
	}

	public void register() {
		this.scheduler = Schedulers.newBoundedElastic(listenerProperties.getMaxThreads(), listenerProperties.getMaxTasks(), listenerName);
		this.queueUrl = getQueueUrl();
		this.disposable = Mono.fromFuture(this::receiveMessages)
				.doOnNext(response -> logger.info("Received {} messages", response.messages().size()))
				.flatMapIterable(ReceiveMessageResponse::messages)
				.repeat()
				.publishOn(scheduler)
				.flatMap(m -> Mono.defer(() -> {
					try {
						targetMethod.invoke(target, m);
						if (listenerProperties.getDeletionPolicy().deleteOnSuccess()) {
							return Mono.fromFuture(() -> deleteMessage(m));
						} else {
							return Mono.empty();
						}
					}
					catch (Exception e) {
						handleError(e, m);
						return Mono.empty();
					}
				}).subscribeOn(scheduler))
				.subscribe();
	}

	private String getQueueUrl() {
		try {
			return sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(listenerProperties.getQueueName()).build())
					.get(5, TimeUnit.SECONDS)
					.queueUrl();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private CompletableFuture<DeleteMessageResponse> deleteMessage(Message m) {
		return sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(m.receiptHandle()).build());
	}

	private void handleError(Throwable ex, Message m) {
		logger.error("Failed to handle message: {}", m, ex);
		if (listenerProperties.getDeletionPolicy().deleteOnError()) {
			try {
				deleteMessage(m).get();
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	private CompletableFuture<ReceiveMessageResponse> receiveMessages() {
		logger.info("Fetching messages [{}]", listenerName);
		return sqs.receiveMessage(ReceiveMessageRequest.builder()
				.queueUrl(queueUrl)
				.maxNumberOfMessages(listenerProperties.getMaxNumberOfMessages())
				.visibilityTimeout(listenerProperties.getVisibilityTimeout())
				.waitTimeSeconds(listenerProperties.getWaitTimeSeconds())
				.build());
	}


	@Override
	public void destroy() throws Exception {
		disposable.dispose();
		scheduler.dispose();
	}
}
