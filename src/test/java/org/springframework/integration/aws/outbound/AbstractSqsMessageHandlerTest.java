package org.springframework.integration.aws.outbound;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.messaging.support.MessageBuilder;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Parent class to contain tests that exercise the SqsMessageHandler class
 *
 * Subclasses can instantiate SqsMessageHandler in their own way.
 *
 * @author Rahul Pilani
 * @author Artem Bilan
 */
public abstract class AbstractSqsMessageHandlerTest {

    @Autowired
    protected AmazonSQS amazonSqs;

    @Autowired
    protected MessageChannel sqsSendChannel;

    @Autowired
    protected SqsMessageHandler sqsMessageHandler;

    @Test
    public void testSqsMessageHandler() {
        Message<String> message = MessageBuilder.withPayload("message").build();
        try {
            this.sqsSendChannel.send(message);
        }
        catch (Exception e) {
            assertThat(e, instanceOf(MessageHandlingException.class));
            assertThat(e.getCause(), instanceOf(IllegalStateException.class));
        }

        this.sqsMessageHandler.setQueue("foo");
        this.sqsSendChannel.send(message);
        ArgumentCaptor<SendMessageRequest> sendMessageRequestArgumentCaptor =
                ArgumentCaptor.forClass(SendMessageRequest.class);
        verify(this.amazonSqs).sendMessage(sendMessageRequestArgumentCaptor.capture());
        assertEquals("http://queue-url.com/foo", sendMessageRequestArgumentCaptor.getValue().getQueueUrl());

        message = MessageBuilder.withPayload("message").setHeader(AwsHeaders.QUEUE, "bar").build();
        this.sqsSendChannel.send(message);
        verify(this.amazonSqs, times(2)).sendMessage(sendMessageRequestArgumentCaptor.capture());
        assertEquals("http://queue-url.com/bar", sendMessageRequestArgumentCaptor.getValue().getQueueUrl());

        SpelExpressionParser spelExpressionParser = new SpelExpressionParser();
        Expression expression = spelExpressionParser.parseExpression("headers.foo");
        this.sqsMessageHandler.setQueueExpression(expression);
        message = MessageBuilder.withPayload("message").setHeader("foo", "baz").build();
        this.sqsSendChannel.send(message);
        verify(this.amazonSqs, times(3)).sendMessage(sendMessageRequestArgumentCaptor.capture());
        assertEquals("http://queue-url.com/baz", sendMessageRequestArgumentCaptor.getValue().getQueueUrl());
    }

}
