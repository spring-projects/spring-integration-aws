/*
 * Copyright 2016-2017 the original author or authors.
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
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

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
import org.springframework.integration.file.remote.FileInfo;
import org.springframework.integration.metadata.SimpleMetadataStore;
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
 * @author Christian Tzolov
 * @author Artem Bilan
 *
 * @since 1.1
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@DirtiesContext
public class S3StreamingChannelAdapterTests {

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	private static final String S3_BUCKET = "S3_BUCKET";

	private static List<S3Object> S3_OBJECTS;

	@Autowired
	private PollableChannel s3FilesChannel;

	@BeforeClass
	public static void setup() throws IOException {
		File remoteFolder = TEMPORARY_FOLDER.newFolder("remote");

		File aFile = new File(remoteFolder, "a.test");
		FileCopyUtils.copy("Hello".getBytes(), aFile);
		File bFile = new File(remoteFolder, "b.test");
		FileCopyUtils.copy("Bye".getBytes(), bFile);

		S3_OBJECTS = new ArrayList<>();

		for (File file : remoteFolder.listFiles()) {
			S3Object s3Object = new S3Object();
			s3Object.setBucketName(S3_BUCKET);
			s3Object.setKey("subdir/" + file.getName());
			s3Object.setObjectContent(new FileInputStream(file));

			S3_OBJECTS.add(s3Object);
		}
	}

	@Test
	public void testS3InboundStreamingChannelAdapter() throws IOException {
		Message<?> message = this.s3FilesChannel.receive(10000);
		assertThat(message).isNotNull();
		assertThat(message.getPayload()).isInstanceOf(InputStream.class);
		assertThat(message.getHeaders().get(FileHeaders.REMOTE_FILE)).isEqualTo("subdir/a.test");

		InputStream inputStreamA = (InputStream) message.getPayload();
		assertThat(inputStreamA).isNotNull();
		assertThat(IOUtils.toString(inputStreamA)).isEqualTo("Hello");

		message = this.s3FilesChannel.receive(10000);
		assertThat(message).isNotNull();
		assertThat(message.getPayload()).isInstanceOf(InputStream.class);
		assertThat(message.getHeaders().get(FileHeaders.REMOTE_FILE)).isEqualTo("subdir/b.test");
		InputStream inputStreamB = (InputStream) message.getPayload();
		assertThat(IOUtils.toString(inputStreamB)).isEqualTo("Bye");

		assertThat(this.s3FilesChannel.receive(10)).isNull();
	}

	@Configuration
	@EnableIntegration
	public static class Config {

		@Bean
		public AmazonS3 amazonS3() {
			AmazonS3 amazonS3 = Mockito.mock(AmazonS3.class);

			willAnswer(invocation -> {
				ObjectListing objectListing = new ObjectListing();
				List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();
				for (S3Object s3Object : S3_OBJECTS) {
					S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
					s3ObjectSummary.setBucketName(S3_BUCKET);
					s3ObjectSummary.setKey(s3Object.getKey());
					s3ObjectSummary.setLastModified(new Date(new File(s3Object.getKey()).lastModified()));
					objectSummaries.add(s3ObjectSummary);
				}
				return objectListing;
			}).given(amazonS3).listObjects(any(ListObjectsRequest.class));

			for (final S3Object s3Object : S3_OBJECTS) {
				willAnswer(invocation -> s3Object).given(amazonS3).getObject(S3_BUCKET, s3Object.getKey());
			}

			return amazonS3;
		}


		@Bean
		@InboundChannelAdapter(value = "s3FilesChannel", poller = @Poller(fixedDelay = "100"))
		public S3StreamingMessageSource s3InboundStreamingMessageSource(AmazonS3 amazonS3) {
			S3SessionFactory s3SessionFactory = new S3SessionFactory(amazonS3);
			S3RemoteFileTemplate s3FileTemplate = new S3RemoteFileTemplate(s3SessionFactory);
			S3StreamingMessageSource s3MessageSource = new S3StreamingMessageSource(s3FileTemplate,
					Comparator.comparing(FileInfo::getFilename));
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
