/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 *     http://duracloud.org/license/
 */
package org.duracloud.tools;

import com.amazonaws.services.elastictranscoder.AmazonElasticTranscoder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Performs tests on the transcoding job generator
 *
 * @author Bill Branan
 * Date: Aug 23, 2018
 */
public class TranscodingJobGeneratorTest {

    private AmazonS3 s3Client;
    private AmazonElasticTranscoder transcoderClient;

    private String awsProfile = "aws-profile";
    private String bucketName = "bucket-name";
    private String pipelineId = "pipeline-id";

    @Before
    public void setup() {
        s3Client = EasyMock.createMock(AmazonS3.class);
        transcoderClient = EasyMock.createMock(AmazonElasticTranscoder.class);
    }

    public void replayMocks() {
        EasyMock.replay(s3Client, transcoderClient);
    }

    @After
    public void teardown() {
        EasyMock.verify(s3Client, transcoderClient);
    }

    /**
     * Tests the transcoding job generator
     *
     * @throws Exception
     */
    @Test
    public void testJobGenerator() throws Exception {
        TranscodingJobGenerator generator =
            new TranscodingJobGenerator(awsProfile, bucketName, pipelineId, false, false);
        generator.setS3Client(s3Client);
        generator.setTranscoderClient(transcoderClient);

        ObjectListing objectListing = new ObjectListing();
        objectListing.setBucketName(bucketName);
        objectListing.setTruncated(false);

        EasyMock.expect(s3Client.listObjects(EasyMock.isA(ListObjectsRequest.class)))
                .andReturn(objectListing);

        // Calls to create jobs

        replayMocks();

        generator.run();
    }

    /**
     * Tests the transcoding job generator with the dry-run option turned on.
     * There should be no calls to create jobs.
     *
     * @throws Exception
     */
    @Test
    public void testDryRun() throws Exception {
        TranscodingJobGenerator generator =
            new TranscodingJobGenerator(awsProfile, bucketName, pipelineId, false, true);
        generator.setS3Client(s3Client);
        generator.setTranscoderClient(transcoderClient);

        ObjectListing objectListing = new ObjectListing();
        objectListing.setBucketName(bucketName);
        objectListing.setTruncated(false);

        EasyMock.expect(s3Client.listObjects(EasyMock.isA(ListObjectsRequest.class)))
                .andReturn(objectListing);

        // No calls to create job

        replayMocks();

        generator.run();
    }

}
