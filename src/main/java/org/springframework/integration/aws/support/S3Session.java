/*
 * Copyright 2002-2019 the original author or authors.
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

package org.springframework.integration.aws.support;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpStatus;

import org.springframework.cloud.aws.core.env.ResourceIdResolver;
import org.springframework.integration.file.remote.session.Session;
import org.springframework.util.Assert;
import org.springframework.util.StreamUtils;
import org.springframework.util.StringUtils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * An Amazon S3 {@link Session} implementation.
 *
 * @author Artem Bilan
 * @author Jim Krygowski
 * @author Anwar Chirakkattil
 */
public class S3Session implements Session<S3ObjectSummary> {

	private final AmazonS3 amazonS3;

	private final ResourceIdResolver resourceIdResolver;

	public S3Session(AmazonS3 amazonS3) {
		this(amazonS3, null);
	}

	public S3Session(AmazonS3 amazonS3, ResourceIdResolver resourceIdResolver) {
		this.resourceIdResolver = resourceIdResolver;
		Assert.notNull(amazonS3, "'amazonS3' must not be null.");
		this.amazonS3 = amazonS3;
	}

	@Override
	public S3ObjectSummary[] list(String path) throws IOException {
		String[] bucketPrefix = splitPathToBucketAndKey(path, false);

		ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketPrefix[0]);
		if (bucketPrefix.length > 1) {
			listObjectsRequest.setPrefix(bucketPrefix[1]);
		}

		/*
		 * For listing objects, Amazon S3 returns up to 1,000 keys in the response. If you
		 * have more than 1,000 keys in your bucket, the response will be truncated. You
		 * should always check for if the response is truncated.
		 */
		ObjectListing objectListing;
		List<S3ObjectSummary> objectSummaries = new ArrayList<>();
		do {
			objectListing = this.amazonS3.listObjects(listObjectsRequest);
			objectSummaries.addAll(objectListing.getObjectSummaries());
			listObjectsRequest.setMarker(objectListing.getNextMarker());
		}
		while (objectListing.isTruncated());

		return objectSummaries.toArray(new S3ObjectSummary[objectSummaries.size()]);
	}

	private String resolveBucket(String bucket) {
		if (this.resourceIdResolver != null) {
			return this.resourceIdResolver.resolveToPhysicalResourceId(bucket);
		}
		else {
			return bucket;
		}
	}

	@Override
	public String[] listNames(String path) throws IOException {
		String[] bucketPrefix = splitPathToBucketAndKey(path, false);

		ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketPrefix[0]);
		if (bucketPrefix.length > 1) {
			listObjectsRequest.setPrefix(bucketPrefix[1]);
		}

		/*
		 * For listing objects, Amazon S3 returns up to 1,000 keys in the response. If you
		 * have more than 1,000 keys in your bucket, the response will be truncated. You
		 * should always check for if the response is truncated.
		 */
		ObjectListing objectListing;
		List<String> names = new ArrayList<>();
		do {
			objectListing = this.amazonS3.listObjects(listObjectsRequest);
			for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
				names.add(objectSummary.getKey());
			}
			listObjectsRequest.setMarker(objectListing.getNextMarker());
		}
		while (objectListing.isTruncated());

		return names.toArray(new String[names.size()]);
	}

	@Override
	public boolean remove(String path) throws IOException {
		String[] bucketKey = splitPathToBucketAndKey(path, true);
		this.amazonS3.deleteObject(bucketKey[0], bucketKey[1]);
		return true;
	}

	@Override
	public void rename(String pathFrom, String pathTo) throws IOException {
		String[] bucketKeyFrom = splitPathToBucketAndKey(pathFrom, true);
		String[] bucketKeyTo = splitPathToBucketAndKey(pathTo, true);
		CopyObjectRequest copyRequest = new CopyObjectRequest(bucketKeyFrom[0], bucketKeyFrom[1], bucketKeyTo[0],
				bucketKeyTo[1]);
		this.amazonS3.copyObject(copyRequest);

		// Delete the source
		this.amazonS3.deleteObject(bucketKeyFrom[0], bucketKeyFrom[1]);
	}

	@Override
	public void read(String source, OutputStream outputStream) throws IOException {
		String[] bucketKey = splitPathToBucketAndKey(source, true);
		S3Object s3Object = this.amazonS3.getObject(bucketKey[0], bucketKey[1]);
		S3ObjectInputStream objectContent = s3Object.getObjectContent();
		try {
			StreamUtils.copy(objectContent, outputStream);
		}
		finally {
			objectContent.close();
		}
	}

	@Override
	public void write(InputStream inputStream, String destination) throws IOException {
		Assert.notNull(inputStream, "'inputStream' must not be null.");
		String[] bucketKey = splitPathToBucketAndKey(destination, true);
		this.amazonS3.putObject(bucketKey[0], bucketKey[1], inputStream, new ObjectMetadata());
	}

	@Override
	public void append(InputStream inputStream, String destination) throws IOException {
		throw new UnsupportedOperationException("The 'append' operation isn't supported by the Amazon S3 protocol.");
	}

	@Override
	public boolean mkdir(String directory) throws IOException {
		this.amazonS3.createBucket(directory);
		return true;
	}

	@Override
	public boolean rmdir(String directory) throws IOException {
		this.amazonS3.deleteBucket(resolveBucket(directory));
		return true;
	}

	@Override
	public boolean exists(String path) throws IOException {
		String[] bucketKey = splitPathToBucketAndKey(path, true);
		try {
			this.amazonS3.getObjectMetadata(bucketKey[0], bucketKey[1]);
		}
		catch (AmazonS3Exception e) {
			if (HttpStatus.SC_NOT_FOUND == e.getStatusCode()) {
				return false;
			}
			else {
				throw e;
			}
		}
		return true;
	}

	@Override
	public InputStream readRaw(String source) throws IOException {
		String[] bucketKey = splitPathToBucketAndKey(source, true);
		S3Object s3Object = this.amazonS3.getObject(bucketKey[0], bucketKey[1]);
		return s3Object.getObjectContent();
	}

	@Override
	public void close() {
		// No-op. This session is just direct wrapper for the AmazonS3
	}

	@Override
	public boolean isOpen() {
		return true;
	}

	@Override
	public boolean finalizeRaw() throws IOException {
		return true;
	}

	@Override
	public Object getClientInstance() {
		return this.amazonS3;
	}

	public String normalizeBucketName(String path) {
		return splitPathToBucketAndKey(path, false)[0];
	}

	private String[] splitPathToBucketAndKey(String path, boolean requireKey) {
		Assert.hasText(path, "'path' must not be empty String.");

		path = StringUtils.trimLeadingCharacter(path, '/');

		String[] bucketKey = path.split("/", 2);

		if (requireKey) {
			Assert.state(bucketKey.length == 2, "'path' must in pattern [BUCKET/KEY].");
			Assert.state(bucketKey[0].length() >= 3, "S3 bucket name must be at least 3 characters long.");
		}
		else {
			Assert.state(bucketKey.length > 0 && bucketKey[0].length() >= 3,
					"S3 bucket name must be at least 3 characters long.");
		}

		bucketKey[0] = resolveBucket(bucketKey[0]);
		return bucketKey;
	}

}
