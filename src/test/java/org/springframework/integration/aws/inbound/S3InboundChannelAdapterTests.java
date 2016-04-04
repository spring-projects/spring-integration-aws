/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.integration.aws.inbound;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Matchers.any;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.hamcrest.Matchers;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.aws.support.filters.S3RegexPatternFileListFilter;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.file.filters.AcceptOnceFileListFilter;
import org.springframework.messaging.Message;
import org.springframework.messaging.PollableChannel;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.FileCopyUtils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * @author Artem Bilan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@DirtiesContext
public class S3InboundChannelAdapterTests {

	private static final ExpressionParser PARSER = new SpelExpressionParser();

	private static final String S3_BUCKET = "S3_BUCKET";

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	private static File REMOTE_FOLDER;

	private static List<S3Object> S3_OBJECTS;

	private static File LOCAL_FOLDER;

	@Autowired
	private PollableChannel s3FilesChannel;

	@BeforeClass
	public static void setup() throws IOException {
		REMOTE_FOLDER = TEMPORARY_FOLDER.newFolder("remote");
		File aFile = new File(REMOTE_FOLDER, "a.test");
		FileCopyUtils.copy("Hello".getBytes(), aFile);
		File bFile = new File(REMOTE_FOLDER, "b.test");
		FileCopyUtils.copy("Bye".getBytes(), bFile);
		File otherFile = new File(REMOTE_FOLDER, "otherFile");
		FileCopyUtils.copy("Other".getBytes(), otherFile);

		S3_OBJECTS = new ArrayList<>();

		for (File file : REMOTE_FOLDER.listFiles()) {
			S3Object s3Object = new S3Object();
			s3Object.setBucketName(S3_BUCKET);
			s3Object.setKey(file.getName());
			s3Object.setObjectContent(new FileInputStream(file));
			S3_OBJECTS.add(s3Object);
		}

		LOCAL_FOLDER = new File(TEMPORARY_FOLDER.getRoot(), "local");
	}

	@Test
	public void testS3InboundChannelAdapter() throws IOException {
		Message<?> message = this.s3FilesChannel.receive(10000);
		assertNotNull(message);
		assertThat(message.getPayload(), instanceOf(File.class));
		File localFile = (File) message.getPayload();
		assertEquals("A.TEST.a", localFile.getName());

		// The test remote files are created with the current timestamp + 1 day.
		assertThat(localFile.lastModified(), Matchers.greaterThan(System.currentTimeMillis()));

		message = this.s3FilesChannel.receive(10000);
		assertNotNull(message);
		assertThat(message.getPayload(), instanceOf(File.class));
		localFile = (File) message.getPayload();
		assertEquals("B.TEST.a", localFile.getName());

		assertThat(localFile.lastModified(), Matchers.greaterThan(System.currentTimeMillis()));

		assertNull(this.s3FilesChannel.receive(10));

		File file = new File(LOCAL_FOLDER, "A.TEST.a");
		assertTrue(file.exists());
		String content = FileCopyUtils.copyToString(new FileReader(file));
		assertEquals("Hello", content);

		file = new File(LOCAL_FOLDER, "B.TEST.a");
		assertTrue(file.exists());
		content = FileCopyUtils.copyToString(new FileReader(file));
		assertEquals("Bye", content);

		assertFalse(new File(LOCAL_FOLDER, "otherFile.a").exists());
	}

	@Configuration
	@EnableIntegration
	public static class Config {

		@Bean
		public AmazonS3 amazonS3() {
			AmazonS3 amazonS3 = Mockito.mock(AmazonS3.class);

			willAnswer(new Answer<ObjectListing>() {

				@Override
				public ObjectListing answer(InvocationOnMock invocation) throws Throwable {
					ObjectListing objectListing = new ObjectListing();
					List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();
					for (S3Object s3Object : S3_OBJECTS) {
						S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
						s3ObjectSummary.setBucketName(S3_BUCKET);
						s3ObjectSummary.setKey(s3Object.getKey());
						Calendar calendar = Calendar.getInstance();
						calendar.add(Calendar.DATE, 1);
						s3ObjectSummary.setLastModified(calendar.getTime());
						objectSummaries.add(s3ObjectSummary);
					}
					return objectListing;
				}

			}).given(amazonS3).listObjects(any(ListObjectsRequest.class));

			for (final S3Object s3Object : S3_OBJECTS) {
				willAnswer(new Answer<S3Object>() {

					@Override
					public S3Object answer(InvocationOnMock invocation) throws Throwable {
						return s3Object;
					}

				}).given(amazonS3).getObject(S3_BUCKET, s3Object.getKey());
			}

			return amazonS3;
		}

		@Bean
		public S3InboundFileSynchronizer s3InboundFileSynchronizer() {
			S3InboundFileSynchronizer synchronizer = new S3InboundFileSynchronizer(amazonS3());
			synchronizer.setDeleteRemoteFiles(true);
			synchronizer.setPreserveTimestamp(true);
			synchronizer.setRemoteDirectory(S3_BUCKET);
			synchronizer.setFilter(new S3RegexPatternFileListFilter(".*\\.test$"));
			Expression expression = PARSER.parseExpression("#this.toUpperCase() + '.a'");
			synchronizer.setLocalFilenameGeneratorExpression(expression);
			return synchronizer;
		}

		@Bean
		@InboundChannelAdapter(value = "s3FilesChannel", poller = @Poller(fixedDelay = "100"))
		public S3InboundFileSynchronizingMessageSource s3InboundFileSynchronizingMessageSource() {
			S3InboundFileSynchronizingMessageSource messageSource =
					new S3InboundFileSynchronizingMessageSource(s3InboundFileSynchronizer());
			messageSource.setAutoCreateLocalDirectory(true);
			messageSource.setLocalDirectory(LOCAL_FOLDER);
			messageSource.setLocalFilter(new AcceptOnceFileListFilter<File>());
			return messageSource;
		}

		@Bean
		public PollableChannel s3FilesChannel() {
			return new QueueChannel();
		}

	}

}
