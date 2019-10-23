/*
 * Copyright 2019 the original author or authors.
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

import java.util.Collections;
import java.util.Map;

import cloud.localstack.docker.annotation.IEnvironmentVariableProvider;
import com.amazonaws.SDKGlobalConfiguration;

/**
 * An {@link IEnvironmentVariableProvider} implementation to provide a {@code USE_SSL}
 * environment variable for docker to start a Local Stack in TLS mode.
 * Also this class populates a {@value SDKGlobalConfiguration#DISABLE_CERT_CHECKING_SYSTEM_PROPERTY}
 * system property to disable SSL certificates validation.
 *
 * @author Artem Bilan
 *
 * @since 2.3
 */
public class LocalStackSslEnvironmentProvider implements IEnvironmentVariableProvider {

	static {
		System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
	}

	@Override
	public Map<String, String> getEnvironmentVariables() {
		return Collections.singletonMap("USE_SSL", "true");
	}

}
