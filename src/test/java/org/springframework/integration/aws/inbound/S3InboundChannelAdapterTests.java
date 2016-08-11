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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Matchers.any;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

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
 * @autor Jim Krygowski
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@DirtiesContext
public class S3InboundChannelAdapterTests {

	private static final ExpressionParser PARSER = new SpelExpressionParser();

	private static final String S3_BUCKET = "S3_BUCKET";

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	private static List<S3Object> S3_OBJECTS;

	private static File LOCAL_FOLDER;

	@Autowired
	private PollableChannel s3FilesChannel;

	@BeforeClass
	public static void setup() throws IOException {
		File remoteFolder = TEMPORARY_FOLDER.newFolder("remote");

		File aFile = new File(remoteFolder, "a.test");
		FileCopyUtils.copy("Hello".getBytes(), aFile);
		File bFile = new File(remoteFolder, "b.test");
		FileCopyUtils.copy("Bye".getBytes(), bFile);
		File otherFile = new File(remoteFolder, "otherFile");
		FileCopyUtils.copy("Other".getBytes(), otherFile);

		S3_OBJECTS = new ArrayList<>();

		for (File file : remoteFolder.listFiles()) {
			S3Object s3Object = new S3Object();
			s3Object.setBucketName(S3_BUCKET);
			s3Object.setKey("subdir/" + file.getName());
			s3Object.setObjectContent(new FileInputStream(file));
			S3_OBJECTS.add(s3Object);
		}

		LOCAL_FOLDER = TEMPORARY_FOLDER.newFolder("local");
	}

	@Test
	public void testS3InboundChannelAdapter() throws IOException {
		Message<?> message = this.s3FilesChannel.receive(10000);
		assertThat(message).isNotNull();
		assertThat(message.getPayload()).isInstanceOf(File.class);
		File localFile = (File) message.getPayload();
		assertThat(localFile.getName()).isEqualTo("A.TEST.a");

		// The test remote files are created with the current timestamp + 1 day.
		assertThat(localFile.lastModified()).isGreaterThan(System.currentTimeMillis());

		message = this.s3FilesChannel.receive(10000);
		assertThat(message).isNotNull();
		assertThat(message.getPayload()).isInstanceOf(File.class);
		localFile = (File) message.getPayload();
		assertThat(localFile.getName()).isEqualTo("B.TEST.a");

		assertThat(localFile.lastModified()).isGreaterThan(System.currentTimeMillis());

		assertThat(this.s3FilesChannel.receive(10)).isNull();

		File file = new File(LOCAL_FOLDER, "A.TEST.a");
		assertThat(file.exists()).isTrue();
		String content = FileCopyUtils.copyToString(new FileReader(file));
		assertThat(content).isEqualTo("Hello");

		file = new File(LOCAL_FOLDER, "B.TEST.a");
		assertThat(file.exists()).isTrue();
		content = FileCopyUtils.copyToString(new FileReader(file));
		assertThat(content).isEqualTo("Bye");

		assertThat(new File(LOCAL_FOLDER, "otherFile.a").exists()).isFalse();
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
			Expression expression = PARSER.parseExpression("(#this.contains('/') ? #this.substring(#this.lastIndexOf('/') + 1) : #this).toUpperCase() + '.a'");
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
