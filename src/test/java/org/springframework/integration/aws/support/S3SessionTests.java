/*
 * Copyright 2016-2020 the original author or authors.
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

package org.springframework.integration.aws.support;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

/**
 * @author Xavier François
 */
class S3SessionTests {

	@Test
	void getHostPortWithoutEndpoint() {

		AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard().withRegion(Regions.EU_WEST_1).build();
		S3SessionFactory s3SessionFactory = new S3SessionFactory(amazonS3);

		assertThat(s3SessionFactory.getSession().getHostPort()).isEqualTo("s3.eu-west-1.amazonaws.com:443");
	}

	@Test
	void getHostPortWithEndpoint() {

		AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard().withRegion(Regions.EU_WEST_1).build();
		S3SessionFactory s3SessionFactory = new S3SessionFactory(amazonS3);
		s3SessionFactory.setEndpoint("s3-url.com:8000");

		assertThat(s3SessionFactory.getSession().getHostPort()).isEqualTo("s3-url.com:8000");
	}
}
