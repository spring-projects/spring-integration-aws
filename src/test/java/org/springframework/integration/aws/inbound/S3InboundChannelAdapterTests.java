/*
 * Copyright 2016-2023 the original author or authors.
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

package org.springframework.integration.aws.inbound;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.aws.support.S3SessionFactory;
import org.springframework.integration.aws.support.filters.S3RegexPatternFileListFilter;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.file.FileHeaders;
import org.springframework.integration.file.filters.AcceptOnceFileListFilter;
import org.springframework.messaging.Message;
import org.springframework.messaging.PollableChannel;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.FileCopyUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.willAnswer;

/**
 * @author Artem Bilan
 * @author Jim Krygowski
 * @author Xavier François
 */
@Disabled("Revise in favor of Local Stack")
@SpringJUnitConfig
@DirtiesContext
public class S3InboundChannelAdapterTests {

	private static final ExpressionParser PARSER = new SpelExpressionParser();

	private static final String S3_BUCKET = "S3_BUCKET";

	@TempDir
	static Path TEMPORARY_FOLDER;

	private static Map<S3Object, File> S3_OBJECTS;

	private static File LOCAL_FOLDER;

	@Autowired
	private PollableChannel s3FilesChannel;

	@BeforeAll
	static void setup() throws IOException {
		File remoteFolder = new File(TEMPORARY_FOLDER.toFile(), "remote");
		remoteFolder.mkdir();

		File aFile = new File(remoteFolder, "a.test");
		aFile.createNewFile();
		FileCopyUtils.copy("Hello".getBytes(), aFile);
		File bFile = new File(remoteFolder, "b.test");
		bFile.createNewFile();
		FileCopyUtils.copy("Bye".getBytes(), bFile);
		File otherFile = new File(remoteFolder, "otherFile");
		otherFile.createNewFile();
		FileCopyUtils.copy("Other".getBytes(), otherFile);

		S3_OBJECTS = new HashMap<>();

		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DATE, 1);

		for (File file : remoteFolder.listFiles()) {
			S3Object s3Object =
					S3Object.builder()
							.key("subdir/" + file.getName())
							.lastModified(calendar.getTime().toInstant())
							.build();
			if (!"otherFile".equals(file.getName())) {
				S3_OBJECTS.put(s3Object, file);
			}
			else {
				S3_OBJECTS.put(s3Object, null);
			}
		}

		LOCAL_FOLDER = TEMPORARY_FOLDER.resolve("local").toFile();
	}

	@Test
	void testS3InboundChannelAdapter() throws IOException {
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

		assertThat(message.getHeaders())
				.containsKeys(FileHeaders.REMOTE_DIRECTORY, FileHeaders.REMOTE_HOST_PORT, FileHeaders.REMOTE_FILE);

		assertThat(message.getHeaders().get(FileHeaders.REMOTE_HOST_PORT)).isEqualTo("s3-url.com:8000");

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
		public S3Client amazonS3() {
			S3Client amazonS3 = Mockito.mock(S3Client.class);

			willAnswer(invocation ->
					ListObjectsResponse.builder()
							.name(S3_BUCKET)
							.contents(S3_OBJECTS.keySet().toArray(new S3Object[0]))
							.build())
					.given(amazonS3)
					.listObjects(any(ListObjectsRequest.class));

			S3_OBJECTS.forEach((s3Object, file) ->
					willAnswer(invocation -> new FileInputStream(file))
							.given(amazonS3)
							.getObject(GetObjectRequest.builder().bucket(S3_BUCKET).key(s3Object.key()).build()));

			return amazonS3;
		}

		@Bean
		public S3SessionFactory s3SessionFactory() {
			S3SessionFactory s3SessionFactory = new S3SessionFactory(amazonS3());
			s3SessionFactory.setEndpoint("s3-url.com:8000");
			return s3SessionFactory;
		}

		@Bean
		public S3InboundFileSynchronizer s3InboundFileSynchronizer() {
			S3InboundFileSynchronizer synchronizer = new S3InboundFileSynchronizer(s3SessionFactory());
			synchronizer.setDeleteRemoteFiles(true);
			synchronizer.setPreserveTimestamp(true);
			synchronizer.setRemoteDirectory(S3_BUCKET);
			synchronizer.setFilter(new S3RegexPatternFileListFilter(".*\\.test$"));
			Expression expression = PARSER.parseExpression(
					"(#this.contains('/') ? #this.substring(#this.lastIndexOf('/') + 1) : #this).toUpperCase() + '.a'");
			synchronizer.setLocalFilenameGeneratorExpression(expression);
			return synchronizer;
		}

		@Bean
		@InboundChannelAdapter(value = "s3FilesChannel", poller = @Poller(fixedDelay = "100"))
		public S3InboundFileSynchronizingMessageSource s3InboundFileSynchronizingMessageSource() {
			S3InboundFileSynchronizingMessageSource messageSource = new S3InboundFileSynchronizingMessageSource(
					s3InboundFileSynchronizer());
			messageSource.setAutoCreateLocalDirectory(true);
			messageSource.setLocalDirectory(LOCAL_FOLDER);
			messageSource.setLocalFilter(new AcceptOnceFileListFilter<>());
			return messageSource;
		}

		@Bean
		public PollableChannel s3FilesChannel() {
			return new QueueChannel();
		}

	}

}
