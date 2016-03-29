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

package org.springframework.integration.aws.config.xml;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.integration.aws.outbound.S3MessageHandler;
import org.springframework.integration.endpoint.EventDrivenConsumer;
import org.springframework.integration.expression.ExpressionUtils;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
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
	private BeanFactory beanFactory;

	@Test
	public void testS3OutboundChannelAdapterParser() {
		assertSame(this.amazonS3,
				TestUtils.getPropertyValue(this.s3OutboundChannelAdapterHandler, "transferManager.s3"));
		assertEquals("foo", TestUtils.getPropertyValue(this.s3OutboundChannelAdapterHandler,
				"bucketExpression.literalValue"));
		assertEquals("'bar'", TestUtils.getPropertyValue(this.s3OutboundChannelAdapterHandler,
				"destinationBucketExpression.expression"));
		assertEquals("'baz'", TestUtils.getPropertyValue(this.s3OutboundChannelAdapterHandler,
				"destinationKeyExpression.expression"));
		assertEquals("payload.name", TestUtils.getPropertyValue(this.s3OutboundChannelAdapterHandler,
				"keyExpression.expression"));
		assertEquals("'qux'", TestUtils.getPropertyValue(this.s3OutboundChannelAdapterHandler,
				"objectAclExpression.expression"));
		assertEquals(S3MessageHandler.Command.COPY.name(),
				TestUtils.getPropertyValue(this.s3OutboundChannelAdapterHandler, "commandExpression.literalValue"));

		assertFalse(TestUtils.getPropertyValue(this.s3OutboundChannelAdapterHandler, "produceReply", Boolean.class));

		assertSame(this.progressListener,
				TestUtils.getPropertyValue(this.s3OutboundChannelAdapterHandler, "s3ProgressListener"));
		assertSame(this.uploadMetadataProvider,
				TestUtils.getPropertyValue(this.s3OutboundChannelAdapterHandler, "uploadMetadataProvider"));

		assertEquals(100, this.s3OutboundChannelAdapter.getPhase());
		assertFalse(this.s3OutboundChannelAdapter.isAutoStartup());
		assertFalse(this.s3OutboundChannelAdapter.isRunning());
		assertSame(this.errorChannel, TestUtils.getPropertyValue(this.s3OutboundChannelAdapter, "inputChannel"));
		assertSame(this.s3OutboundChannelAdapterHandler,
				TestUtils.getPropertyValue(this.s3OutboundChannelAdapter, "handler"));
	}

	@Test
	public void testS3OutboundGatewayParser() {
		assertSame(this.transferManager,
				TestUtils.getPropertyValue(this.s3OutboundGatewayHandler, "transferManager"));
		assertEquals("'FOO'", TestUtils.getPropertyValue(this.s3OutboundGatewayHandler,
				"bucketExpression.expression"));
		Expression commandExpression =
				TestUtils.getPropertyValue(this.s3OutboundGatewayHandler, "commandExpression", Expression.class);
		assertEquals("'" + S3MessageHandler.Command.DOWNLOAD.name() + "'",
				TestUtils.getPropertyValue(commandExpression, "expression"));

		StandardEvaluationContext evaluationContext = ExpressionUtils.createStandardEvaluationContext(this.beanFactory);
		S3MessageHandler.Command command =
				commandExpression.getValue(evaluationContext, S3MessageHandler.Command.class);

		assertEquals(S3MessageHandler.Command.DOWNLOAD, command);

		assertTrue(TestUtils.getPropertyValue(this.s3OutboundGatewayHandler, "produceReply", Boolean.class));
		assertSame(this.nullChannel, TestUtils.getPropertyValue(this.s3OutboundGatewayHandler, "outputChannel"));

		assertTrue(this.s3OutboundGateway.isRunning());
		assertSame(this.errorChannel, TestUtils.getPropertyValue(this.s3OutboundGateway, "inputChannel"));
		assertSame(this.s3OutboundGatewayHandler, TestUtils.getPropertyValue(this.s3OutboundGateway, "handler"));
	}

}
