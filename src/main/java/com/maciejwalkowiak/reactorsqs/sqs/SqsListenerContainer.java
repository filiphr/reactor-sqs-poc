package com.maciejwalkowiak.reactorsqs.sqs;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.maciejwalkowiak.reactorsqs.sqs.SqsProperties.ListenerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

public class SqsListenerContainer implements DisposableBean {
	private static final Logger logger = LoggerFactory.getLogger(SqsListenerContainer.class);

	private final Object monitor = new Object();
	private final String listenerName;
	private final ListenerProperties listenerProperties;
	private final SqsAsyncClient sqs;
	private final Object target;
	private final Method targetMethod;

	private AsyncTaskExecutor asyncTaskExecutor;
	private Future<?> future;

	private String queueUrl;

	public SqsListenerContainer(String listenerName, ListenerProperties listenerProperties, SqsAsyncClient sqs, Object target, Method targetMethod) {
		this.listenerName = listenerName;
		this.listenerProperties = listenerProperties;
		this.sqs = sqs;
		this.target = target;
		this.targetMethod = targetMethod;
	}

	public void register() {
		this.queueUrl = getQueueUrl();
		ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
		threadPoolTaskExecutor.setCorePoolSize(listenerProperties.getMaxThreads());
		threadPoolTaskExecutor.setMaxPoolSize(listenerProperties.getMaxThreads());
		threadPoolTaskExecutor.setAllowCoreThreadTimeOut(true);
		threadPoolTaskExecutor.afterPropertiesSet();
		this.asyncTaskExecutor = threadPoolTaskExecutor;

		synchronized (monitor) {
			future = asyncTaskExecutor.submit(new ReceiveMessagesRunnable());
		}
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

	private void handleSuccess(Message m) {
		logger.debug("Successfully to handles message: {}", m);
		if (listenerProperties.getDeletionPolicy().deleteOnSuccess()) {
			try {
				deleteMessage(m).get();
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	private void submitExecution(Runnable runnable) {
		synchronized (monitor) {
			future = asyncTaskExecutor.submit(runnable);
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

	private class ReceiveMessagesRunnable implements Runnable {

		@Override
		public void run() {
			receiveMessages()
					.thenApplyAsync(response -> {
						List<Message> messages = response.messages();
						logger.info("Received {} messages", messages.size());
						return messages;
					}, asyncTaskExecutor)
					.exceptionally(throwable -> {
						logger.error("Failed to receive messages.", throwable);
						return Collections.emptyList();
					})
					.thenCompose(messages -> {
						Collection<CompletableFuture<Void>> messageInvocations = new ArrayList<>(messages.size());

						for (Message message : messages) {
							messageInvocations.add(CompletableFuture.runAsync(new InvokeMethodRunnable(message), asyncTaskExecutor));
						}

						return CompletableFuture.allOf(messageInvocations.toArray(new CompletableFuture[0]));
					})
					.handle((unused, throwable) -> {
						if (throwable != null) {
							logger.error("Error while handling messages on queue: {}", queueUrl, throwable);
						}
						resubmitExecution();
						return null;
					});
		}

		private void resubmitExecution() {
			submitExecution(this);
		}
	}

	private class InvokeMethodRunnable implements Runnable {

		private final Message message;

		private InvokeMethodRunnable(Message message) {
			this.message = message;
		}

		@Override
		public void run() {
			try {
				targetMethod.invoke(target, message);
			}
			catch (Exception e) {
				handleError(e, message);
			}
			if (listenerProperties.getDeletionPolicy().deleteOnSuccess()) {
				handleSuccess(message);
			}
		}
	}


	@Override
	public void destroy() throws Exception {
		synchronized (monitor) {
			if (future != null) {
				future.cancel(true);
				future = null;
			}
		}
		((ThreadPoolTaskExecutor) asyncTaskExecutor).destroy();
	}
}
