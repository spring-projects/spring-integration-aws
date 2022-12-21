/*
 * Copyright 2016-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.integration.aws.outbound;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import com.amazonaws.AmazonClientException;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.internal.Mimetypes;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SetObjectAclRequest;
import com.amazonaws.services.s3.transfer.ObjectMetadataProvider;
import com.amazonaws.services.s3.transfer.PersistableTransfer;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.internal.S3ProgressListener;
import com.amazonaws.services.s3.transfer.internal.S3ProgressListenerChain;
import com.amazonaws.util.Base64;
import com.amazonaws.util.Md5Utils;
import io.awspring.cloud.core.env.ResourceIdResolver;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.expression.ExpressionUtils;
import org.springframework.integration.expression.ValueExpression;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.DigestUtils;

/**
 * The {@link AbstractReplyProducingMessageHandler} implementation for the Amazon S3
 * services.
 * <p>
 * The implementation is fully based on the {@link TransferManager} and support its
 * {@code upload}, {@code download} and {@code copy} operations which can be determined by
 * the provided or evaluated via SpEL expression at runtime
 * {@link S3MessageHandler.Command}.
 * <p>
 * This {@link AbstractReplyProducingMessageHandler} can behave as a "one-way" (by
 * default) or "request-reply" component according to the {@link #produceReply}
 * constructor argument.
 * <p>
 * The "one-way" behavior is also blocking, which is achieved with the
 * {@link Transfer#waitForException()} invocation. Consider to use an async upstream hand
 * off if this blocking behavior isn't appropriate.
 * <p>
 * The "request-reply" behavior is async and the {@link Transfer} result from the
 * {@link TransferManager} operation is sent to the {@link #getOutputChannel()}, assuming
 * the transfer progress observation in the downstream flow.
 * <p>
 * The {@link S3ProgressListener} can be supplied to track the transfer progress. Also the
 * listener can be populated into the returned {@link Transfer} afterwards in the
 * downstream flow. If the context of the {@code requestMessage} is important in the
 * {@code progressChanged} event, it is recommended to use a
 * {@link MessageS3ProgressListener} implementation instead. *
 * <p>
 * For the upload operation the {@link UploadMetadataProvider} callback can be supplied to
 * populate required {@link ObjectMetadata} options, as for a single entry, as well as for
 * each file in directory to upload.
 * <p>
 * For the upload operation the {@link #objectAclExpression} can be provided to
 * {@link AmazonS3#setObjectAcl} after the successful transfer. The supported SpEL result
 * types are: {@link AccessControlList} or {@link CannedAccessControlList}.
 * <p>
 * For download operation the {@code payload} must be a {@link File} instance,
 * representing a single file for downloaded content or directory to download all files
 * from the S3 virtual directory.
 * <p>
 * An S3 Object {@code key} for upload and download can be determined by the provided
 * {@link #keyExpression} or the {@link File#getName()} is used directly. The former has
 * precedence.
 * <p>
 * For copy operation all {@link #keyExpression}, {@link #destinationBucketExpression} and
 * {@link #destinationKeyExpression} are required and must not evaluate to {@code null}.
 *
 * @author Artem Bilan
 * @author John Logan
 *
 * @see TransferManager
 */
public class S3MessageHandler extends AbstractReplyProducingMessageHandler {

	private final TransferManager transferManager;

	private final boolean produceReply;

	private final Expression bucketExpression;

	private EvaluationContext evaluationContext;

	private Expression keyExpression;

	private Expression objectAclExpression;

	private Expression destinationBucketExpression;

	private Expression destinationKeyExpression;

	private Expression commandExpression = new ValueExpression<>(Command.UPLOAD);

	private S3ProgressListener s3ProgressListener;

	private UploadMetadataProvider uploadMetadataProvider;

	private ResourceIdResolver resourceIdResolver;

	public S3MessageHandler(AmazonS3 amazonS3, String bucket) {
		this(amazonS3, bucket, false);
	}

	public S3MessageHandler(AmazonS3 amazonS3, Expression bucketExpression) {
		this(amazonS3, bucketExpression, false);
	}

	public S3MessageHandler(AmazonS3 amazonS3, String bucket, boolean produceReply) {
		this(amazonS3, new LiteralExpression(bucket), produceReply);
		Assert.notNull(bucket, "'bucket' must not be null");
	}

	public S3MessageHandler(AmazonS3 amazonS3, Expression bucketExpression, boolean produceReply) {
		this(TransferManagerBuilder.standard().withS3Client(amazonS3).build(), bucketExpression, produceReply);
		Assert.notNull(amazonS3, "'amazonS3' must not be null");
	}

	public S3MessageHandler(TransferManager transferManager, String bucket) {
		this(transferManager, bucket, false);
	}

	public S3MessageHandler(TransferManager transferManager, Expression bucketExpression) {
		this(transferManager, bucketExpression, false);
	}

	public S3MessageHandler(TransferManager transferManager, String bucket, boolean produceReply) {
		this(transferManager, new LiteralExpression(bucket), produceReply);
		Assert.notNull(bucket, "'bucket' must not be null");
	}

	public S3MessageHandler(TransferManager transferManager, Expression bucketExpression, boolean produceReply) {
		Assert.notNull(transferManager, "'transferManager' must not be null");
		Assert.notNull(bucketExpression, "'bucketExpression' must not be null");
		this.transferManager = transferManager;
		this.bucketExpression = bucketExpression;
		this.produceReply = produceReply;
	}

	/**
	 * The SpEL expression to evaluate S3 object key at runtime against
	 * {@code requestMessage}.
	 * @param keyExpression the SpEL expression for S3 key.
	 */
	public void setKeyExpression(Expression keyExpression) {
		this.keyExpression = keyExpression;
	}

	/**
	 * The SpEL expression to evaluate S3 object ACL at runtime against
	 * {@code requestMessage} for the {@code upload} operation.
	 * @param objectAclExpression the SpEL expression for S3 object ACL.
	 */
	public void setObjectAclExpression(Expression objectAclExpression) {
		this.objectAclExpression = objectAclExpression;
	}

	/**
	 * Specify a {@link S3MessageHandler.Command} to perform against
	 * {@link TransferManager}.
	 * @param command The {@link S3MessageHandler.Command} to use.
	 * @see S3MessageHandler.Command
	 */
	public void setCommand(Command command) {
		Assert.notNull(command, "'command' must not be null");
		setCommandExpression(new ValueExpression<>(command));
	}

	/**
	 * The SpEL expression to evaluate the command to perform on {@link TransferManager}:
	 * {@code upload}, {@code download} or {@code copy}.
	 * @param commandExpression the SpEL expression to evaluate the
	 * {@link TransferManager} operation.
	 * @see Command
	 */
	public void setCommandExpression(Expression commandExpression) {
		Assert.notNull(commandExpression, "'commandExpression' must not be null");
		this.commandExpression = commandExpression;
	}

	/**
	 * The SpEL expression to evaluate the target S3 bucket for copy operation.
	 * @param destinationBucketExpression the SpEL expression for destination bucket.
	 * @see TransferManager#copy(String, String, String, String)
	 */
	public void setDestinationBucketExpression(Expression destinationBucketExpression) {
		this.destinationBucketExpression = destinationBucketExpression;
	}

	/**
	 * The SpEL expression to evaluate the target S3 key for copy operation.
	 * @param destinationKeyExpression the SpEL expression for destination key.
	 * @see TransferManager#copy(String, String, String, String)
	 */
	public void setDestinationKeyExpression(Expression destinationKeyExpression) {
		this.destinationKeyExpression = destinationKeyExpression;
	}

	/**
	 * Specify a {@link S3ProgressListener} for upload and download operations.
	 * @param s3ProgressListener the {@link S3ProgressListener} to use.
	 * @see MessageS3ProgressListener
	 */
	public void setProgressListener(S3ProgressListener s3ProgressListener) {
		this.s3ProgressListener = s3ProgressListener;
	}

	/**
	 * Specify an {@link ObjectMetadata} callback to populate the metadata for upload
	 * operation, e.g. {@code Content-MD5}, {@code Content-Type} or any other required
	 * options.
	 * @param uploadMetadataProvider the {@link UploadMetadataProvider} to use for upload.
	 */
	public void setUploadMetadataProvider(UploadMetadataProvider uploadMetadataProvider) {
		this.uploadMetadataProvider = uploadMetadataProvider;
	}

	/**
	 * Specify a {@link ResourceIdResolver} to resolve logical bucket names to physical
	 * resource ids.
	 * @param resourceIdResolver the {@link ResourceIdResolver} to use.
	 */
	public void setResourceIdResolver(ResourceIdResolver resourceIdResolver) {
		this.resourceIdResolver = resourceIdResolver;
	}

	@Override
	protected void doInit() {
		Assert.notNull(this.bucketExpression, "The 'bucketExpression' must not be null");
		super.doInit();
		this.evaluationContext = ExpressionUtils.createStandardEvaluationContext(getBeanFactory());
	}

	@Override
	protected Object handleRequestMessage(Message<?> requestMessage) {
		Command command = this.commandExpression.getValue(this.evaluationContext, requestMessage, Command.class);
		Assert.state(command != null, () -> "'commandExpression' [" + this.commandExpression.getExpressionString()
				+ "] cannot evaluate to null.");

		Transfer transfer = null;

		switch (command) {
		case UPLOAD:
			transfer = upload(requestMessage);
			break;

		case DOWNLOAD:
			transfer = download(requestMessage);
			break;

		case COPY:
			transfer = copy(requestMessage);
			break;
		}

		if (this.produceReply) {
			return transfer;
		}
		else {
			try {
				AmazonClientException amazonClientException = transfer.waitForException();
				if (amazonClientException != null) {
					throw amazonClientException;
				}
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			return null;
		}
	}

	private Transfer upload(Message<?> requestMessage) {
		Object payload = requestMessage.getPayload();
		String bucketName = obtainBucket(requestMessage);

		String key = null;
		if (this.keyExpression != null) {
			key = this.keyExpression.getValue(this.evaluationContext, requestMessage, String.class);
		}

		if (payload instanceof File && ((File) payload).isDirectory()) {
			File fileToUpload = (File) payload;
			if (key == null) {
				key = fileToUpload.getName();
			}
			return this.transferManager.uploadDirectory(bucketName, key, fileToUpload, true,
					new MessageHeadersObjectMetadataProvider(requestMessage.getHeaders()));
		}
		else {
			ObjectMetadata metadata = new ObjectMetadata();
			if (this.uploadMetadataProvider != null) {
				this.uploadMetadataProvider.populateMetadata(metadata, requestMessage);
			}

			PutObjectRequest putObjectRequest;

			try {
				if (payload instanceof InputStream) {
					InputStream inputStream = (InputStream) payload;
					if (metadata.getContentMD5() == null) {
						Assert.state(inputStream.markSupported(),
								"For an upload InputStream with no MD5 digest metadata, "
										+ "the markSupported() method must evaluate to true.");
						byte[] md5Digest = DigestUtils.md5Digest(inputStream);
						metadata.setContentMD5(Base64.encodeAsString(md5Digest));
						inputStream.reset();
					}
					putObjectRequest = new PutObjectRequest(bucketName, key, inputStream, metadata);
				}
				else if (payload instanceof File) {
					File fileToUpload = (File) payload;
					if (key == null) {
						key = fileToUpload.getName();
					}
					if (metadata.getContentMD5() == null) {
						String contentMd5 = Md5Utils.md5AsBase64(fileToUpload);
						metadata.setContentMD5(contentMd5);
					}
					if (metadata.getContentLength() == 0) {
						metadata.setContentLength(fileToUpload.length());
					}
					if (metadata.getContentType() == null) {
						metadata.setContentType(Mimetypes.getInstance().getMimetype(fileToUpload));
					}
					putObjectRequest = new PutObjectRequest(bucketName, key, fileToUpload).withMetadata(metadata);
				}
				else if (payload instanceof byte[]) {
					byte[] payloadBytes = (byte[]) payload;
					InputStream inputStream = new ByteArrayInputStream(payloadBytes);
					if (metadata.getContentMD5() == null) {
						String contentMd5 = Md5Utils.md5AsBase64(inputStream);
						metadata.setContentMD5(contentMd5);
						inputStream.reset();
					}
					if (metadata.getContentLength() == 0) {
						metadata.setContentLength(payloadBytes.length);
					}
					putObjectRequest = new PutObjectRequest(bucketName, key, inputStream, metadata);
				}
				else {
					throw new IllegalArgumentException("Unsupported payload type: [" + payload.getClass()
							+ "]. The only supported payloads for the upload request are "
							+ "java.io.File, java.io.InputStream, byte[] and PutObjectRequest.");
				}
			}
			catch (IOException e) {
				throw new MessageHandlingException(requestMessage, e);
			}

			if (key == null) {
				if (this.keyExpression != null) {
					throw new IllegalStateException("The 'keyExpression' [" + this.keyExpression.getExpressionString()
							+ "] must not evaluate to null. Root object is: " + requestMessage);
				}
				else {
					throw new IllegalStateException("Specify a 'keyExpression' for non-java.io.File payloads");
				}
			}

			S3ProgressListener configuredProgressListener = this.s3ProgressListener;
			if (this.s3ProgressListener instanceof MessageS3ProgressListener) {
				configuredProgressListener = new S3ProgressListener() {

					@Override
					public void onPersistableTransfer(PersistableTransfer persistableTransfer) {
						S3MessageHandler.this.s3ProgressListener.onPersistableTransfer(persistableTransfer);
					}

					@Override
					public void progressChanged(ProgressEvent progressEvent) {
						((MessageS3ProgressListener) S3MessageHandler.this.s3ProgressListener)
								.progressChanged(progressEvent, requestMessage);
					}

				};
			}

			S3ProgressListener progressListener = configuredProgressListener;

			if (this.objectAclExpression != null) {
				Object acl = this.objectAclExpression.getValue(this.evaluationContext, requestMessage);
				Assert.state(acl == null || acl instanceof AccessControlList || acl instanceof CannedAccessControlList,
						() -> "The 'objectAclExpression' [" + this.objectAclExpression.getExpressionString()
								+ "] must evaluate to com.amazonaws.services.s3.model.AccessControlList "
								+ "or must evaluate to com.amazonaws.services.s3.model.CannedAccessControlList. "
								+ "Gotten: [" + acl + "]");

				SetObjectAclRequest aclRequest;

				if (acl instanceof AccessControlList) {
					aclRequest = new SetObjectAclRequest(bucketName, key, (AccessControlList) acl);
				}
				else {
					aclRequest = new SetObjectAclRequest(bucketName, key, (CannedAccessControlList) acl);
				}

				final SetObjectAclRequest theAclRequest = aclRequest;
				progressListener = new S3ProgressListener() {

					@Override
					public void onPersistableTransfer(PersistableTransfer persistableTransfer) {

					}

					@Override
					public void progressChanged(ProgressEvent progressEvent) {
						if (ProgressEventType.TRANSFER_COMPLETED_EVENT.equals(progressEvent.getEventType())) {
							S3MessageHandler.this.transferManager.getAmazonS3Client().setObjectAcl(theAclRequest);
						}
					}

				};

				if (configuredProgressListener != null) {
					progressListener = new S3ProgressListenerChain(configuredProgressListener, progressListener);
				}

			}

			if (progressListener != null) {
				return this.transferManager.upload(putObjectRequest, progressListener);
			}
			else {
				return this.transferManager.upload(putObjectRequest);
			}
		}
	}

	private Transfer download(Message<?> requestMessage) {
		Object payload = requestMessage.getPayload();
		Assert.state(payload instanceof File, () -> "For the 'DOWNLOAD' operation the 'payload' must be of "
				+ "'java.io.File' type, but gotten: [" + payload.getClass() + ']');

		File targetFile = (File) payload;

		String bucket = obtainBucket(requestMessage);

		String key = null;
		if (this.keyExpression != null) {
			key = this.keyExpression.getValue(this.evaluationContext, requestMessage, String.class);
		}
		else {
			key = targetFile.getName();
		}

		Assert.state(key != null,
				() -> "The 'keyExpression' must not be null for non-File payloads and can't evaluate to null. "
						+ "Root object is: " + requestMessage);

		if (targetFile.isDirectory()) {
			return this.transferManager.downloadDirectory(bucket, key, targetFile);
		}
		else {
			if (this.s3ProgressListener != null) {
				return this.transferManager.download(new GetObjectRequest(bucket, key), targetFile,
						this.s3ProgressListener);
			}
			else {
				return this.transferManager.download(bucket, key, targetFile);
			}
		}
	}

	private Transfer copy(Message<?> requestMessage) {
		String sourceBucketName = obtainBucket(requestMessage);

		String sourceKey = null;
		if (this.keyExpression != null) {
			sourceKey = this.keyExpression.getValue(this.evaluationContext, requestMessage, String.class);
		}

		Assert.state(sourceKey != null, () -> "The 'keyExpression' must not be null for 'copy' operation "
				+ "and 'keyExpression' can't evaluate to null. " + "Root object is: " + requestMessage);

		String destinationBucketName = null;
		if (this.destinationBucketExpression != null) {
			destinationBucketName = this.destinationBucketExpression.getValue(this.evaluationContext, requestMessage,
					String.class);
		}

		if (this.resourceIdResolver != null) {
			destinationBucketName = this.resourceIdResolver.resolveToPhysicalResourceId(destinationBucketName);
		}

		Assert.state(destinationBucketName != null,
				() -> "The 'destinationBucketExpression' must not be null for 'copy' operation "
						+ "and can't evaluate to null. Root object is: " + requestMessage);

		String destinationKey = null;
		if (this.destinationKeyExpression != null) {
			destinationKey = this.destinationKeyExpression.getValue(this.evaluationContext, requestMessage,
					String.class);
		}

		Assert.state(destinationKey != null,
				() -> "The 'destinationKeyExpression' must not be null for 'copy' operation "
						+ "and can't evaluate to null. Root object is: " + requestMessage);

		CopyObjectRequest copyObjectRequest = new CopyObjectRequest(sourceBucketName, sourceKey, destinationBucketName,
				destinationKey);
		return this.transferManager.copy(copyObjectRequest);
	}

	private String obtainBucket(Message<?> requestMessage) {
		String bucketName;
		if (this.bucketExpression instanceof LiteralExpression) {
			bucketName = (String) this.bucketExpression.getValue();
		}
		else {
			bucketName = this.bucketExpression.getValue(this.evaluationContext, requestMessage, String.class);
		}
		Assert.state(bucketName != null, () -> "The 'bucketExpression' [" + this.bucketExpression.getExpressionString()
				+ "] must not evaluate to null. Root object is: " + requestMessage);

		if (this.resourceIdResolver != null) {
			bucketName = this.resourceIdResolver.resolveToPhysicalResourceId(bucketName);
		}

		return bucketName;
	}

	/**
	 * The {@link S3MessageHandler} mode.
	 *
	 * @see #setCommand
	 */
	public enum Command {

		/**
		 * The command to perform {@link TransferManager#upload} operation.
		 */
		UPLOAD,

		/**
		 * The command to perform {@link TransferManager#download} operation.
		 */
		DOWNLOAD,

		/**
		 * The command to perform {@link TransferManager#copy} operation.
		 */
		COPY

	}

	/**
	 * An {@link S3ProgressListener} extension to provide a {@code requestMessage} context
	 * for the {@code progressChanged} event.
	 *
	 * @since 2.1
	 */
	public interface MessageS3ProgressListener extends S3ProgressListener {

		@Override
		default void progressChanged(ProgressEvent progressEvent) {
			throw new UnsupportedOperationException("Use progressChanged(ProgressEvent, Message<?>) instead.");
		}

		void progressChanged(ProgressEvent progressEvent, Message<?> message);

	}

	/**
	 * The callback to populate an {@link ObjectMetadata} for upload operation. The
	 * message can be used as a metadata source.
	 */
	public interface UploadMetadataProvider {

		void populateMetadata(ObjectMetadata metadata, Message<?> message);

	}

	private class MessageHeadersObjectMetadataProvider implements ObjectMetadataProvider {

		private final MessageHeaders messageHeaders;

		MessageHeadersObjectMetadataProvider(MessageHeaders messageHeaders) {
			this.messageHeaders = messageHeaders;
		}

		@Override
		public void provideObjectMetadata(File file, ObjectMetadata metadata) {
			if (S3MessageHandler.this.uploadMetadataProvider != null) {
				S3MessageHandler.this.uploadMetadataProvider.populateMetadata(metadata,
						MessageBuilder.createMessage(file, this.messageHeaders));
			}
			if (metadata.getContentMD5() == null) {
				try {
					String contentMd5 = Md5Utils.md5AsBase64(file);
					metadata.setContentMD5(contentMd5);
				}
				catch (Exception e) {
					throw new AmazonClientException(e);
				}
			}
		}

	}

}
