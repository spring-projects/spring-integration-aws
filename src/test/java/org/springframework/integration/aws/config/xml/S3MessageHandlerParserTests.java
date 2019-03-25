/*
 * Copyright 2016-2019 the original author or authors.
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

package org.springframework.integration.aws.config.xml;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cloud.aws.core.env.ResourceIdResolver;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.integration.aws.outbound.S3MessageHandler;
import org.springframework.integration.endpoint.EventDrivenConsumer;
import org.springframework.integration.expression.ExpressionUtils;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.internal.S3ProgressListener;

/**
 * @author Artem Bilan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@DirtiesContext
public class S3MessageHandlerParserTests {

	@Autowired
	private AmazonS3 amazonS3;

	@Autowired
	private TransferManager transferManager;

	@Autowired
	private MessageChannel errorChannel;

	@Autowired
	private MessageChannel nullChannel;

	@Autowired
	private EventDrivenConsumer s3OutboundChannelAdapter;

	@Autowired
	@Qualifier("s3OutboundChannelAdapter.handler")
	private MessageHandler s3OutboundChannelAdapterHandler;

	@Autowired
	private EventDrivenConsumer s3OutboundGateway;

	@Autowired
	@Qualifier("s3OutboundGateway.handler")
	private MessageHandler s3OutboundGatewayHandler;

	@Autowired
	private S3ProgressListener progressListener;

	@Autowired
	private S3MessageHandler.UploadMetadataProvider uploadMetadataProvider;

	@Autowired
	private ResourceIdResolver resourceIdResolver;

	@Autowired
	private BeanFactory beanFactory;

	@Test
	public void testS3OutboundChannelAdapterParser() {
		assertThat(TestUtils.getPropertyValue(this.s3OutboundChannelAdapterHandler, "transferManager.s3"))
				.isSameAs(this.amazonS3);
		assertThat(TestUtils.getPropertyValue(this.s3OutboundChannelAdapterHandler,
				"bucketExpression.literalValue"))
				.isEqualTo("foo");
		assertThat(TestUtils.getPropertyValue(this.s3OutboundChannelAdapterHandler,
				"destinationBucketExpression.expression"))
				.isEqualTo("'bar'");
		assertThat(TestUtils.getPropertyValue(this.s3OutboundChannelAdapterHandler,
				"destinationKeyExpression.expression"))
				.isEqualTo("'baz'");
		assertThat(TestUtils.getPropertyValue(this.s3OutboundChannelAdapterHandler,
				"keyExpression.expression"))
				.isEqualTo("payload.name");
		assertThat(TestUtils.getPropertyValue(this.s3OutboundChannelAdapterHandler,
				"objectAclExpression.expression"))
				.isEqualTo("'qux'");
		assertThat(TestUtils.getPropertyValue(this.s3OutboundChannelAdapterHandler, "commandExpression.literalValue"))
				.isEqualTo(S3MessageHandler.Command.COPY.name());

		assertThat(TestUtils.getPropertyValue(this.s3OutboundChannelAdapterHandler, "produceReply", Boolean.class))
				.isFalse();

		assertThat(TestUtils.getPropertyValue(this.s3OutboundChannelAdapterHandler, "s3ProgressListener"))
				.isSameAs(this.progressListener);
		assertThat(TestUtils.getPropertyValue(this.s3OutboundChannelAdapterHandler, "uploadMetadataProvider"))
				.isSameAs(this.uploadMetadataProvider);
		assertThat(TestUtils.getPropertyValue(this.s3OutboundChannelAdapterHandler, "resourceIdResolver"))
				.isSameAs(this.resourceIdResolver);

		assertThat(this.s3OutboundChannelAdapter.getPhase()).isEqualTo(100);
		assertThat(this.s3OutboundChannelAdapter.isAutoStartup()).isFalse();
		assertThat(this.s3OutboundChannelAdapter.isRunning()).isFalse();
		assertThat(TestUtils.getPropertyValue(this.s3OutboundChannelAdapter, "inputChannel"))
				.isSameAs(this.errorChannel);
		assertThat(TestUtils.getPropertyValue(this.s3OutboundChannelAdapter, "handler"))
				.isSameAs(this.s3OutboundChannelAdapterHandler);
	}

	@Test
	public void testS3OutboundGatewayParser() {
		assertThat(TestUtils.getPropertyValue(this.s3OutboundGatewayHandler, "transferManager"))
				.isSameAs(this.transferManager);
		assertThat(TestUtils.getPropertyValue(this.s3OutboundGatewayHandler,
				"bucketExpression.expression"))
				.isEqualTo("'FOO'");
		Expression commandExpression =
				TestUtils.getPropertyValue(this.s3OutboundGatewayHandler, "commandExpression", Expression.class);
		assertThat(TestUtils.getPropertyValue(commandExpression, "expression"))
				.isEqualTo("'" + S3MessageHandler.Command.DOWNLOAD.name() + "'");

		StandardEvaluationContext evaluationContext = ExpressionUtils.createStandardEvaluationContext(this.beanFactory);
		S3MessageHandler.Command command =
				commandExpression.getValue(evaluationContext, S3MessageHandler.Command.class);

		assertThat(command).isEqualTo(S3MessageHandler.Command.DOWNLOAD);

		assertThat(TestUtils.getPropertyValue(this.s3OutboundGatewayHandler, "produceReply", Boolean.class)).isTrue();
		assertThat(TestUtils.getPropertyValue(this.s3OutboundGatewayHandler, "outputChannel"))
				.isSameAs(this.nullChannel);

		assertThat(this.s3OutboundGateway.isRunning()).isTrue();
		assertThat(TestUtils.getPropertyValue(this.s3OutboundGateway, "inputChannel")).isSameAs(this.errorChannel);
		assertThat(TestUtils.getPropertyValue(this.s3OutboundGateway, "handler"))
				.isSameAs(this.s3OutboundGatewayHandler);
	}

}
