/*
 * Copyright 2017-2019 the original author or authors.
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

package org.springframework.integration.aws;

import static org.junit.Assume.assumeNoException;
import static org.junit.Assume.assumeTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClientBuilder;

/**
 * The {@link TestWatcher} implementation for local Amazon Kinesis service. See
 * https://github.com/mhart/kinesalite.
 *
 * @author Artem Bilan
 * @since 1.1
 */
public final class KinesisLocalRunning extends TestWatcher {

	public static final int DEFAULT_PORT = 4568;

	private static Log logger = LogFactory.getLog(KinesisLocalRunning.class);

	// Static so that we only test once on failure: speeds up test suite
	private static Map<Integer, Boolean> kinesisOnline = new HashMap<>();

	private final int port;

	private AmazonKinesisAsync amazonKinesis;

	private KinesisLocalRunning(int port) {
		this.port = port;
		kinesisOnline.put(port, true);
	}

	public AmazonKinesisAsync getKinesis() {
		return this.amazonKinesis;
	}

	@Override
	public Statement apply(Statement base, Description description) {
		assumeTrue(kinesisOnline.get(this.port));

		String url = "http://localhost:" + this.port;

		// See https://github.com/mhart/kinesalite#cbor-protocol-issues-with-the-java-sdk
		System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");

		this.amazonKinesis = AmazonKinesisAsyncClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("", "")))
				.withClientConfiguration(new ClientConfiguration().withMaxErrorRetry(0).withConnectionTimeout(1000))
				.withEndpointConfiguration(
						new AwsClientBuilder.EndpointConfiguration(url, Regions.DEFAULT_REGION.getName()))
				.build();

		try {
			this.amazonKinesis.listStreams();
		}
		catch (SdkClientException e) {
			logger.warn("Tests not running because no Kinesis on " + url, e);
			assumeNoException(e);
		}

		return new Statement() {

			@Override
			public void evaluate() throws Throwable {
				try {
					base.evaluate();
				}
				finally {
					System.clearProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY);
				}

			}
		};
	}

	public static KinesisLocalRunning isRunning() {
		return isRunning(DEFAULT_PORT);
	}

	public static KinesisLocalRunning isRunning(int port) {
		return new KinesisLocalRunning(port);
	}

}
