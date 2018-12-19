/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.integration.aws.support;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * Converts a future of type &lt;S&gt; to a future of type &lt;T&gt; using the given conversion function.
 *
 * @param <S> type of the source future
 * @param <T> type of the target future
 *
 * @author Artem Bilan
 * @since 2.1.0
 */
public class FutureConverter<S, T> implements Future<T> {

	private Future<S> source;
	private Function<S, T> conversionFunction;

	public FutureConverter(Future<S> source, Function<S, T> conversionFunction) {
		this.source = Objects.requireNonNull(source);
		this.conversionFunction = Objects.requireNonNull(conversionFunction);
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return this.source.cancel(mayInterruptIfRunning);
	}

	@Override
	public boolean isCancelled() {
		return this.source.isCancelled();
	}

	@Override
	public boolean isDone() {
		return this.source.isDone();
	}

	@Override
	public T get() throws InterruptedException, ExecutionException {
		return this.conversionFunction.apply(this.source.get());
	}

	@Override
	public T get(long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		return this.conversionFunction.apply(this.source.get(timeout, unit));
	}
}
