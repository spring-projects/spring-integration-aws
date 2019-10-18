package org.springframework.integration.aws;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;

import cloud.localstack.LocalstackTestRunner;
import cloud.localstack.TestUtils;
import com.amazonaws.services.s3.AmazonS3;

@RunWith(LocalstackTestRunner.class)
public class MyCloudAppTests {


	@Test
	public void testLocalS3API() {
		AmazonS3 s3 = TestUtils.getClientS3();
		assertThat(s3).isNotNull();
	}

}
