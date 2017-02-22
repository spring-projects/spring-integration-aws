/*
 * Copyright 2015-2016 the original author or authors.
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

package org.springframework.integration.aws.core;

import org.junit.Assert;
import org.junit.Test;

import com.amazonaws.regions.Regions;

/**
 * @author Blake Jackson
 */
public class RegionsTests {

	@Test
	public void testUsEast2RegionExists() {
		Assert.assertNotNull(Regions.US_EAST_2);
		Assert.assertEquals("us-east-2", Regions.US_EAST_2.getName());
	}
}
