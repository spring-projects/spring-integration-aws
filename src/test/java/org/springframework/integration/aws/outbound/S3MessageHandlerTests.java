/*
 * Copyright 2016 the original author or authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.springframework.integration.aws.outbound;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.http.client.methods.HttpRequestBase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.http.MediaType;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.expression.ValueExpression;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.FileCopyUtils;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SetObjectAclRequest;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.PersistableTransfer;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.internal.S3ProgressListener;
import com.amazonaws.services.s3.transfer.internal.S3ProgressPublisher;
import com.amazonaws.util.BinaryUtils;
import com.amazonaws.util.Md5Utils;
import com.amazonaws.util.StringInputStream;
import com.amazonaws.util.StringUtils;

/**
 * @author Artem Bilan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@DirtiesContext
public class S3MessageHandlerTests {

	private static SpelExpressionParser PARSER = new SpelExpressionParser();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Autowired
	private AmazonS3 amazonS3;

	@Autowired
	private MessageChannel s3SendChannel;

	@Autowired
	private CountDownLatch transferCompletedLatch;

	@Autowired
	private CountDownLatch aclLatch;

	@Autowired
	private MessageChannel s3ProcessChannel;

	@Autowired
	private PollableChannel s3ReplyChannel;

	@Test
	public void testUploadFile() throws IOException, InterruptedException {
		File file = this.temporaryFolder.newFile("foo.mp3");
		Message<?> message = MessageBuilder.withPayload(file)
				.setHeader("s3Command", S3MessageHandler.Command.UPLOAD.name())
				.build();

		this.s3SendChannel.send(message);

		ArgumentCaptor<PutObjectRequest> putObjectRequestArgumentCaptor =
				ArgumentCaptor.forClass(PutObjectRequest.class);
		verify(this.amazonS3, atLeastOnce()).putObject(putObjectRequestArgumentCaptor.capture());

		PutObjectRequest putObjectRequest = putObjectRequestArgumentCaptor.getValue();
		assertEquals("myBucket", putObjectRequest.getBucketName());
		assertEquals("foo.mp3", putObjectRequest.getKey());
		assertNull(putObjectRequest.getFile());
		assertNotNull(putObjectRequest.getInputStream());

		ObjectMetadata metadata = putObjectRequest.getMetadata();
		assertEquals(Md5Utils.md5AsBase64(file), metadata.getContentMD5());
		assertEquals(0, metadata.getContentLength());
		assertEquals("audio/mpeg", metadata.getContentType());

		ProgressListener listener = putObjectRequest.getGeneralProgressListener();
		S3ProgressPublisher.publishProgress(listener, ProgressEventType.TRANSFER_COMPLETED_EVENT);

		assertTrue(this.transferCompletedLatch.await(10, TimeUnit.SECONDS));
		assertTrue(this.aclLatch.await(10, TimeUnit.SECONDS));

		ArgumentCaptor<SetObjectAclRequest> setObjectAclRequestArgumentCaptor =
				ArgumentCaptor.forClass(SetObjectAclRequest.class);
		verify(this.amazonS3).setObjectAcl(setObjectAclRequestArgumentCaptor.capture());

		SetObjectAclRequest setObjectAclRequest = setObjectAclRequestArgumentCaptor.getValue();

		assertEquals("myBucket", setObjectAclRequest.getBucketName());
		assertEquals("foo.mp3", setObjectAclRequest.getKey());
		assertNull(setObjectAclRequest.getAcl());
		assertEquals(CannedAccessControlList.PublicReadWrite, setObjectAclRequest.getCannedAcl());
	}

	@Test
	public void testUploadInputStream() throws IOException {
		InputStream payload = new StringInputStream("a");
		Message<?> message = MessageBuilder.withPayload(payload)
				.setHeader("s3Command", S3MessageHandler.Command.UPLOAD.name())
				.setHeader("key", "myStream")
				.build();

		this.s3SendChannel.send(message);

		ArgumentCaptor<PutObjectRequest> putObjectRequestArgumentCaptor =
				ArgumentCaptor.forClass(PutObjectRequest.class);
		verify(this.amazonS3, atLeastOnce()).putObject(putObjectRequestArgumentCaptor.capture());

		PutObjectRequest putObjectRequest = putObjectRequestArgumentCaptor.getValue();
		assertEquals("myBucket", putObjectRequest.getBucketName());
		assertEquals("myStream", putObjectRequest.getKey());
		assertNull(putObjectRequest.getFile());
		assertNotNull(putObjectRequest.getInputStream());

		ObjectMetadata metadata = putObjectRequest.getMetadata();
		assertEquals(Md5Utils.md5AsBase64(payload), metadata.getContentMD5());
		assertEquals(1, metadata.getContentLength());
		assertEquals(MediaType.APPLICATION_JSON_VALUE, metadata.getContentType());
		assertEquals("test.json", metadata.getContentDisposition());
	}

	@Test
	public void testDownloadDirectory() throws IOException {
		File directoryForDownload = this.temporaryFolder.newFolder("myFolder");
		Message<?> message = MessageBuilder.withPayload(directoryForDownload)
				.setHeader("s3Command", S3MessageHandler.Command.DOWNLOAD)
				.build();

		this.s3SendChannel.send(message);

		File[] fileArray = directoryForDownload.listFiles();
		assertNotNull(fileArray);
		assertEquals(2, fileArray.length);

		List<File> files = Arrays.asList(fileArray);
		Collections.sort(files, new Comparator<File>() {

			@Override
			public int compare(File o1, File o2) {
				return o1.getName().compareTo(o2.getName());
			}

		});

		File file1 = files.get(0);
		assertEquals("bar", file1.getName());
		assertEquals("bb", FileCopyUtils.copyToString(new FileReader(file1)));

		File file2 = files.get(1);
		assertEquals("foo", file2.getName());
		assertEquals("f", FileCopyUtils.copyToString(new FileReader(file2)));
	}

	@Test
	public void testCopy() throws InterruptedException {
		Map<String, String> payload = new HashMap<>();
		payload.put("key", "mySource");
		payload.put("destination", "theirBucket");
		payload.put("destinationKey", "theirTarget");
		this.s3ProcessChannel.send(new GenericMessage<>(payload));

		Message<?> receive = this.s3ReplyChannel.receive(10000);
		assertNotNull(receive);

		Assert.assertThat(receive.getPayload(), instanceOf(Copy.class));
		Copy copy = (Copy) receive.getPayload();
		assertEquals("Copying object from myBucket/mySource to theirBucket/theirTarget", copy.getDescription());

		copy.waitForCompletion();

		assertEquals(Transfer.TransferState.Completed, copy.getState());
	}

	@Configuration
	@EnableIntegration
	public static class ContextConfiguration {

		@Bean
		public AmazonS3 amazonS3() {
			AmazonS3 amazonS3 = mock(AmazonS3.class);

			when(amazonS3.putObject(any(PutObjectRequest.class))).thenReturn(new PutObjectResult());
			when(amazonS3.getObjectMetadata(any(GetObjectMetadataRequest.class))).thenReturn(new ObjectMetadata());
			when(amazonS3.copyObject(any(CopyObjectRequest.class))).thenReturn(new CopyObjectResult());

			ObjectListing objectListing = spy(new ObjectListing());

			List<S3ObjectSummary> s3ObjectSummaries = new LinkedList<>();

			S3ObjectSummary fileSummary1 = new S3ObjectSummary();
			fileSummary1.setBucketName("myBucket");
			fileSummary1.setKey("foo");
			fileSummary1.setSize(1);
			s3ObjectSummaries.add(fileSummary1);

			S3ObjectSummary fileSummary2 = new S3ObjectSummary();
			fileSummary2.setBucketName("myBucket");
			fileSummary2.setKey("bar");
			fileSummary2.setSize(2);
			s3ObjectSummaries.add(fileSummary2);

			when(objectListing.getObjectSummaries()).thenReturn(s3ObjectSummaries);
			when(amazonS3.listObjects(any(ListObjectsRequest.class))).thenReturn(objectListing);

			final S3Object file1 = new S3Object();
			file1.setBucketName("myBucket");
			file1.setKey("foo");
			try {
				byte[] data = "f".getBytes(StringUtils.UTF8);
				byte[] md5 = Md5Utils.computeMD5Hash(data);
				file1.getObjectMetadata().setHeader(Headers.ETAG, BinaryUtils.toHex(md5));
				S3ObjectInputStream content = new S3ObjectInputStream(new ByteArrayInputStream(data),
						mock(HttpRequestBase.class));
				file1.setObjectContent(content);
			}
			catch (Exception e) {
				// no-op
			}

			final S3Object file2 = new S3Object();
			file2.setBucketName("myBucket");
			file2.setKey("bar");
			try {
				byte[] data = "bb".getBytes(StringUtils.UTF8);
				byte[] md5 = Md5Utils.computeMD5Hash(data);
				file2.getObjectMetadata().setHeader(Headers.ETAG, BinaryUtils.toHex(md5));
				S3ObjectInputStream content = new S3ObjectInputStream(new ByteArrayInputStream(data),
						mock(HttpRequestBase.class));
				file2.setObjectContent(content);
			}
			catch (Exception e) {
				// no-op
			}

			doAnswer(new Answer<S3Object>() {

				@Override
				public S3Object answer(InvocationOnMock invocation) throws Throwable {
					GetObjectRequest getObjectRequest = (GetObjectRequest) invocation.getArguments()[0];
					String key = getObjectRequest.getKey();
					if ("foo".equals(key)) {
						return file1;
					}
					else if ("bar".equals(key)) {
						return file2;
					}
					else {
						return (S3Object) invocation.callRealMethod();
					}
				}

			}).when(amazonS3).getObject(any(GetObjectRequest.class));

			doAnswer(new Answer<Object>() {

				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					aclLatch().countDown();
					return null;
				}

			}).when(amazonS3).setObjectAcl(any(SetObjectAclRequest.class));

			return amazonS3;
		}


		@Bean
		public CountDownLatch aclLatch() {
			return new CountDownLatch(1);
		}

		@Bean
		public CountDownLatch transferCompletedLatch() {
			return new CountDownLatch(1);
		}

		@Bean
		public S3ProgressListener s3ProgressListener() {
			return new S3ProgressListener() {

				@Override
				public void onPersistableTransfer(PersistableTransfer persistableTransfer) {

				}

				@Override
				public void progressChanged(ProgressEvent progressEvent) {
					if (ProgressEventType.TRANSFER_COMPLETED_EVENT.equals(progressEvent.getEventType())) {
						transferCompletedLatch().countDown();
					}
				}

			};
		}

		@Bean
		@ServiceActivator(inputChannel = "s3SendChannel")
		public MessageHandler s3MessageHandler() {
			S3MessageHandler s3MessageHandler = new S3MessageHandler(amazonS3(), "myBucket");
			s3MessageHandler.setCommandExpression(PARSER.parseExpression("headers.s3Command"));
			s3MessageHandler.setKeyExpression(PARSER.parseExpression("payload instanceof T(java.io.File) ? payload.name : headers.key"));
			s3MessageHandler.setObjectAclExpression(new ValueExpression<>(CannedAccessControlList.PublicReadWrite));
			s3MessageHandler.setUploadMetadataProvider(new S3MessageHandler.UploadMetadataProvider() {

				@Override
				public void populateMetadata(ObjectMetadata metadata, Message<?> message) {
					if (message.getPayload() instanceof InputStream) {
						metadata.setContentLength(1);
						metadata.setContentType(MediaType.APPLICATION_JSON_VALUE);
						metadata.setContentDisposition("test.json");
					}
				}

			});
			s3MessageHandler.setS3ProgressListener(s3ProgressListener());
			return s3MessageHandler;
		}

		@Bean
		public PollableChannel s3ReplyChannel() {
			return new QueueChannel();
		}

		@Bean
		@ServiceActivator(inputChannel = "s3ProcessChannel")
		public MessageHandler s3ProcessMessageHandler() {
			S3MessageHandler s3MessageHandler = new S3MessageHandler(amazonS3(), "myBucket", true);
			s3MessageHandler.setOutputChannel(s3ReplyChannel());
			s3MessageHandler.setCommand(S3MessageHandler.Command.COPY);
			s3MessageHandler.setKeyExpression(PARSER.parseExpression("payload.key"));
			s3MessageHandler.setDestinationBucketExpression(PARSER.parseExpression("payload.destination"));
			s3MessageHandler.setDestinationKeyExpression(PARSER.parseExpression("payload.destinationKey"));
			return s3MessageHandler;
		}

	}

}
