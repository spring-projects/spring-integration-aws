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
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
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
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.aws.support.S3RemoteFileTemplate;
import org.springframework.integration.aws.support.S3SessionFactory;
import org.springframework.integration.aws.support.filters.S3PersistentAcceptOnceFileListFilter;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.file.FileHeaders;
import org.springframework.integration.metadata.SimpleMetadataStore;
import org.springframework.messaging.Message;
import org.springframework.messaging.PollableChannel;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.FileCopyUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.willAnswer;

/**
 * @author Christian Tzolov
 * @author Artem Bilan
 *
 * @since 1.1
 */
@Disabled("Revise in favor of Local Stack")
@SpringJUnitConfig
@DirtiesContext
public class S3StreamingChannelAdapterTests {

	private static final String S3_BUCKET = "S3_BUCKET";

	@TempDir
	static Path TEMPORARY_FOLDER;

	private static Map<S3Object, File> S3_OBJECTS;

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

		S3_OBJECTS = new HashMap<>();

		for (File file : remoteFolder.listFiles()) {
			S3Object s3Object =
					S3Object.builder()
							.key("subdir/" + file.getName())
							.lastModified(Instant.ofEpochMilli(file.lastModified()))
							.build();
			S3_OBJECTS.put(s3Object, file);
		}
	}

	@Test
	void testS3InboundStreamingChannelAdapter() throws IOException {
		Message<?> message = this.s3FilesChannel.receive(10000);
		assertThat(message).isNotNull();
		assertThat(message.getPayload()).isInstanceOf(InputStream.class);
		assertThat(message.getHeaders().get(FileHeaders.REMOTE_FILE)).isEqualTo("subdir/a.test");

		InputStream inputStreamA = (InputStream) message.getPayload();
		assertThat(inputStreamA).isNotNull();
		assertThat(IOUtils.toString(inputStreamA, Charset.defaultCharset())).isEqualTo("Hello");
		inputStreamA.close();

		message = this.s3FilesChannel.receive(10000);
		assertThat(message).isNotNull();
		assertThat(message.getPayload()).isInstanceOf(InputStream.class);
		assertThat(message.getHeaders().get(FileHeaders.REMOTE_FILE)).isEqualTo("subdir/b.test");
		assertThat(message.getHeaders())
				.containsKeys(FileHeaders.REMOTE_DIRECTORY, FileHeaders.REMOTE_HOST_PORT, FileHeaders.REMOTE_FILE);
		InputStream inputStreamB = (InputStream) message.getPayload();
		assertThat(IOUtils.toString(inputStreamB, Charset.defaultCharset())).isEqualTo("Bye");

		assertThat(this.s3FilesChannel.receive(10)).isNull();

		inputStreamB.close();
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
		@InboundChannelAdapter(value = "s3FilesChannel", poller = @Poller(fixedDelay = "100"))
		public S3StreamingMessageSource s3InboundStreamingMessageSource(S3Client amazonS3) {
			S3SessionFactory s3SessionFactory = new S3SessionFactory(amazonS3);
			S3RemoteFileTemplate s3FileTemplate = new S3RemoteFileTemplate(s3SessionFactory);
			S3StreamingMessageSource s3MessageSource =
					new S3StreamingMessageSource(s3FileTemplate, Comparator.comparing(S3Object::key));
			s3MessageSource.setRemoteDirectory("/" + S3_BUCKET + "/subdir");
			s3MessageSource.setFilter(new S3PersistentAcceptOnceFileListFilter(new SimpleMetadataStore(), "streaming"));

			return s3MessageSource;
		}

		@Bean
		public PollableChannel s3FilesChannel() {
			return new QueueChannel();
		}

	}

}
