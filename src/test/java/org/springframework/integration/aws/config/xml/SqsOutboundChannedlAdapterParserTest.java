package org.springframework.integration.aws.config.xml;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.integration.aws.outbound.SqsMessageHandler;
import org.springframework.integration.test.util.TestUtils;

/**
 * @author Rahul Pilani
 */
public class SqsOutboundChannedlAdapterParserTest {

    @Test(expected = BeanDefinitionStoreException.class)
    public void test_sqs_resource_resolver_defined_with_queue_messaging_template() {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("org/springframework/integration/aws/config/xml/SqsOutboundChannelAdapterParserTests-context-bad.xml");
        applicationContext.getBean("sqsOutboundChannelAdapter");
    }

    @Test(expected = BeanDefinitionStoreException.class)
    public void test_sqs_defined_with_queue_messaging_template() {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("org/springframework/integration/aws/config/xml/SqsOutboundChannelAdapterParserTests-context-bad2.xml");
        applicationContext.getBean("sqsOutboundChannelAdapter");
    }

    @Test(expected = BeanDefinitionStoreException.class)
    public void test_resource_resolver_defined_with_queue_messaging_template() {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("org/springframework/integration/aws/config/xml/SqsOutboundChannelAdapterParserTests-context-bad3.xml");
        applicationContext.getBean("sqsOutboundChannelAdapter");
    }

    @Test
    public void test_happy_path_with_queue_messaging_template() {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("org/springframework/integration/aws/config/xml/SqsOutboundChannelAdapterParserTests-context-good.xml");
        SqsMessageHandler handlerWithTemplate = (SqsMessageHandler) applicationContext.getBean("sqsOutboundChannelAdapterWithQueueMessagingTemplate.handler");
        Assert.assertNotNull(TestUtils.getPropertyValue(handlerWithTemplate, "template"));

        SqsMessageHandler handlerWithSqs = (SqsMessageHandler) applicationContext.getBean("sqsOutboundChannelAdapterWithSqs.handler");
        Assert.assertNotNull(TestUtils.getPropertyValue(handlerWithSqs, "template"));
    }


}
