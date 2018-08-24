/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 *     http://duracloud.org/license/
 */
package org.duracloud.tools;

import static org.duracloud.tools.TranscodingJobGenerator.AUDIO_PRESET_ID;
import static org.duracloud.tools.TranscodingJobGenerator.PLAYLIST_FORMAT;
import static org.duracloud.tools.TranscodingJobGenerator.SEGMENT_DURATION;
import static org.duracloud.tools.TranscodingJobGenerator.VIDEO_PRESET_ID;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.elastictranscoder.AmazonElasticTranscoder;
import com.amazonaws.services.elastictranscoder.model.CreateJobRequest;
import com.amazonaws.services.elastictranscoder.model.CreateJobResult;
import com.amazonaws.services.elastictranscoder.model.Job;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.easymock.Capture;
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
public class TranscodingJobGeneratorTest extends JobGeneratorTestBase {

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
            new TranscodingJobGenerator(bucketName, pipelineId, false, false);
        generator.setS3Client(s3Client);
        generator.setTranscoderClient(transcoderClient);

        // Add expectations for listObject calls on S3
        expectListObjects();

        // Calls to create jobs
        CreateJobResult createJobResult = new CreateJobResult().withJob(new Job().withStatus("Pending"));

        Capture<CreateJobRequest> createAudioJobRequestCapture = Capture.newInstance();
        EasyMock.expect(transcoderClient.createJob(EasyMock.capture(createAudioJobRequestCapture)))
                .andReturn(createJobResult);

        Capture<CreateJobRequest> createVideoJobRequestCapture = Capture.newInstance();
        EasyMock.expect(transcoderClient.createJob(EasyMock.capture(createVideoJobRequestCapture)))
                .andReturn(createJobResult);

        replayMocks();

        generator.run();

        // Verify audio request
        CreateJobRequest audioJobRequest = createAudioJobRequestCapture.getValue();
        assertEquals(pipelineId, audioJobRequest.getPipelineId());
        assertEquals("audio.mp3", audioJobRequest.getInput().getKey());
        assertEquals(AUDIO_PRESET_ID, audioJobRequest.getOutput().getPresetId());
        assertEquals("audio-a160k", audioJobRequest.getOutput().getKey());
        assertEquals(SEGMENT_DURATION, audioJobRequest.getOutput().getSegmentDuration());
        assertEquals("audio-playlist", audioJobRequest.getPlaylists().get(0).getName());
        assertEquals(PLAYLIST_FORMAT, audioJobRequest.getPlaylists().get(0).getFormat());
        assertEquals("audio-a160k", audioJobRequest.getPlaylists().get(0).getOutputKeys().get(0));

        // Verify video request
        CreateJobRequest videoJobRequest = createVideoJobRequestCapture.getValue();
        assertEquals(pipelineId, videoJobRequest.getPipelineId());
        assertEquals("video.mp4", videoJobRequest.getInput().getKey());
        assertEquals(AUDIO_PRESET_ID, videoJobRequest.getOutputs().get(0).getPresetId());
        assertEquals("video-a160k", videoJobRequest.getOutputs().get(0).getKey());
        assertEquals(SEGMENT_DURATION, videoJobRequest.getOutputs().get(0).getSegmentDuration());
        assertEquals(VIDEO_PRESET_ID, videoJobRequest.getOutputs().get(1).getPresetId());
        assertEquals("video-v2m", videoJobRequest.getOutputs().get(1).getKey());
        assertEquals(SEGMENT_DURATION, videoJobRequest.getOutputs().get(1).getSegmentDuration());
        assertEquals("video-playlist", videoJobRequest.getPlaylists().get(0).getName());
        assertEquals(PLAYLIST_FORMAT, videoJobRequest.getPlaylists().get(0).getFormat());
        assertEquals("video-a160k", videoJobRequest.getPlaylists().get(0).getOutputKeys().get(0));
        assertEquals("video-v2m", videoJobRequest.getPlaylists().get(0).getOutputKeys().get(1));
    }

    private void expectListObjects() {
        // First object listing includes 2 files, an audio file and a video file

        // MP3 audio file
        S3ObjectSummary objSummmaryAudio = new S3ObjectSummary();
        objSummmaryAudio.setBucketName(bucketName);
        objSummmaryAudio.setKey("audio.mp3");

        // MP4
        S3ObjectSummary objSummmaryVideo = new S3ObjectSummary();
        objSummmaryVideo.setBucketName(bucketName);
        objSummmaryVideo.setKey("video.mp4");

        List<S3ObjectSummary> objectSummaries = new ArrayList<>();
        objectSummaries.add(objSummmaryAudio);
        objectSummaries.add(objSummmaryVideo);

        SettableObjectListing objectListing = new SettableObjectListing();
        objectListing.setBucketName(bucketName);
        objectListing.setObjectSummaries(objectSummaries);
        objectListing.setTruncated(false);

        EasyMock.expect(s3Client.listObjects(EasyMock.isA(ListObjectsRequest.class)))
                .andReturn(objectListing);

        // Second object listing includes 0 files, an empty list
        SettableObjectListing objectListingEmpty = new SettableObjectListing();
        List<S3ObjectSummary> objectSummariesEmpty = new ArrayList<>();
        objectListingEmpty.setObjectSummaries(objectSummariesEmpty);
        objectListingEmpty.setTruncated(false);

        EasyMock.expect(s3Client.listObjects(EasyMock.isA(ListObjectsRequest.class)))
                .andReturn(objectListingEmpty);
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

        expectListObjects();

        // No calls to create jobs (dry run)

        replayMocks();

        generator.run();
    }

}
