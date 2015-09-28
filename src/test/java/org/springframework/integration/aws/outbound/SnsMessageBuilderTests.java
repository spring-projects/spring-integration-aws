/*
 * Copyright 2016 the original author or authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.springframework.integration.aws.outbound;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.junit.Test;

import org.springframework.integration.aws.support.SnsBodyBuilder;

/**
 * @author Artem Bilan
 */
public class SnsMessageBuilderTests {

	@Test
	public void testSnsMessageBuilder() {
		try {
			SnsBodyBuilder.withDefault("");
			fail("IllegalArgumentException expected");
		}
		catch (Exception e) {
			assertThat(e, instanceOf(IllegalArgumentException.class));
			assertThat(e.getMessage(), containsString("defaultMessage must not be empty."));
		}

		String message = SnsBodyBuilder.withDefault("foo").build();
		assertEquals("{\"default\":\"foo\"}", message);

		try {
			SnsBodyBuilder.withDefault("foo")
					.forProtocols("{\"foo\" : \"bar\"}")
					.build();
			fail("IllegalArgumentException expected");
		}
		catch (Exception e) {
			assertThat(e, instanceOf(IllegalArgumentException.class));
			assertThat(e.getMessage(), containsString("protocols must not be empty."));
		}
		try {
			SnsBodyBuilder.withDefault("foo")
					.forProtocols("{\"foo\" : \"bar\"}", "")
					.build();
			fail("IllegalArgumentException expected");
		}
		catch (Exception e) {
			assertThat(e, instanceOf(IllegalArgumentException.class));
			assertThat(e.getMessage(), containsString("protocols must not contain empty elements."));
		}

		message = SnsBodyBuilder.withDefault("foo")
				.forProtocols("{\"foo\" : \"bar\"}", "sms")
				.build();

		assertEquals("{\"default\":\"foo\",\"sms\":\"{\\\"foo\\\" : \\\"bar\\\"}\"}", message);
	}

}
