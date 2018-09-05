package org.duracloud.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elastictranscoder.AmazonElasticTranscoder;
import com.amazonaws.services.elastictranscoder.AmazonElasticTranscoderClientBuilder;
import com.amazonaws.services.elastictranscoder.model.CreateJobOutput;
import com.amazonaws.services.elastictranscoder.model.CreateJobPlaylist;
import com.amazonaws.services.elastictranscoder.model.CreateJobRequest;
import com.amazonaws.services.elastictranscoder.model.CreateJobResult;
import com.amazonaws.services.elastictranscoder.model.JobInput;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.util.StringUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.AutoCloseInputStream;

/*
 * Transcoding Job Generator - Tool for creating Elastic Transcoder Jobs
 *                             for content in an S3 bucket.
 *
 * @author: Bill Branan
 * Date: Aug 23, 2018
 */
public class TranscodingJobGenerator {

    private String bucketName;
    private String filePath;
    private String pipelineId;
    private boolean verbose;
    private boolean dryRun;

    private static Options cmdOptions;

    private AmazonS3 s3Client;
    private AmazonElasticTranscoder transcoderClient;

    /*
     * Elastic Transcoder System Presets are listed here:
     * https://docs.aws.amazon.com/elastictranscoder/latest/developerguide/system-presets.html
     */
    // System Preset: HLS Audio - 160k
    protected static final String AUDIO_PRESET_ID = "1351620000001-200060";
    // System Preset: HLS Video - 2M
    protected static final String VIDEO_PRESET_ID = "1351620000001-200015";

    // Using Apple recommended segment duration, see:
    // https://developer.apple.com/documentation/http_live_streaming/hls_authoring_specification_for_apple_devices
    protected static final String SEGMENT_DURATION = "6";

    // Using playlist format from latest HLS version
    protected static final String PLAYLIST_FORMAT = "HLSv4";

    /**
     * Use to set up tool with AWS clients that are based on a locally defined profile
     */
    public TranscodingJobGenerator(String awsCredentialsProfileName,
                                   String bucketName,
                                   String filePath,
                                   String pipelineId,
                                   boolean verbose,
                                   boolean dryRun) {
        this.bucketName = bucketName;
        this.filePath = filePath;
        this.pipelineId = pipelineId;
        this.verbose = verbose;
        this.dryRun = dryRun;

        String contentListSource = "\nbucket name=" + bucketName;
        if (null == bucketName) {
            contentListSource = "\nfile path=" + filePath;
        }

        System.out.println("-----------------------------------------" +
                           "\nRunning Transcoding Job Generator with config:" +
                           "\nAWS profile=" + awsCredentialsProfileName +
                           contentListSource +
                           "\npipeline ID=" + pipelineId +
                           "\nverbose=" + verbose +
                           (dryRun ? "\nThis execution is a DRY RUN - no jobs will be created!" : "") +
                           "\n-----------------------------------------");

        AWSCredentialsProvider credentialsProvider =
            new ProfileCredentialsProvider(awsCredentialsProfileName);
        setS3Client(
            AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider).build());
        setTranscoderClient(
            AmazonElasticTranscoderClientBuilder.standard().withCredentials(credentialsProvider).build());
    }

    /**
     * Use to create tool and set AWS clients independently (e.g. for testing)
     */
    protected TranscodingJobGenerator(String bucketName,
                                      String pipelineId,
                                      boolean verbose,
                                      boolean dryRun) {
        this.bucketName = bucketName;
        this.pipelineId = pipelineId;
        this.verbose = verbose;
        this.dryRun = dryRun;
    }

    protected void setS3Client(AmazonS3 s3Client) {
        this.s3Client = s3Client;
    }

    protected void setTranscoderClient(AmazonElasticTranscoder transcoderClient) {
        this.transcoderClient = transcoderClient;
    }

    /**
     * Kicks off the execution of the tool.
     *
     * @throws IOException
     */
    public void run() throws IOException {

        if (null != bucketName) { // Get list of items from bucket
            createJobs(new ContentIterator(s3Client, bucketName));
        } else { // Get list of items from file
            List<String> contentItems = new LinkedList<>();

            File file = new File(filePath);
            if (!file.exists()) {
                throw new FileNotFoundException("The content item list file does not exist at " + filePath);
            }

            try (BufferedReader is = new BufferedReader(
                new InputStreamReader(new AutoCloseInputStream(FileUtils.openInputStream(file))))) {

                String line = null;
                while ((line = is.readLine()) != null) {
                    String contentId = line.trim();
                    contentItems.add(contentId);
                }
            }

            createJobs(contentItems.iterator());
        }
    }

    private void createJobs(Iterator<String> contentIterator) {
        int filesProcessed = 0;
        int jobsCreated = 0;

        while (contentIterator.hasNext()) {
            String contentId = contentIterator.next();
            boolean jobCreated = createJob(contentId);
            filesProcessed++;

            if (jobCreated) {
                jobsCreated++;
            }
        }

        System.out.println("\nTranscoding Job Generator process complete. " +
                           filesProcessed + " files processed, " + jobsCreated + " jobs created");
    }

    private boolean createJob(String contentId) {
        CreateJobRequest createJobRequest;

        if (contentId.endsWith(".mp3")) { // Audio file
            String outputKey = StringUtils.replace(contentId, ".mp3", "-a160k");
            String playlistName = StringUtils.replace(contentId, ".mp3", "-playlist");

            createJobRequest = new CreateJobRequest()
                .withPipelineId(pipelineId)
                .withInput(new JobInput().withKey(contentId))
                .withOutput(new CreateJobOutput().withPresetId(AUDIO_PRESET_ID)
                                                 .withKey(outputKey)
                                                 .withSegmentDuration(SEGMENT_DURATION))
                .withPlaylists(new CreateJobPlaylist().withName(playlistName)
                                                      .withFormat(PLAYLIST_FORMAT)
                                                      .withOutputKeys(outputKey));
        } else if (contentId.endsWith(".mp4")) { // Video file
            String audioOutputKey = StringUtils.replace(contentId, ".mp4", "-a160k");
            String videoOutputKey = StringUtils.replace(contentId, ".mp4", "-v2m");
            String playlistName = StringUtils.replace(contentId, ".mp4", "-playlist");

            createJobRequest = new CreateJobRequest()
                .withPipelineId(pipelineId)
                .withInput(new JobInput().withKey(contentId))
                .withOutputs(new CreateJobOutput().withPresetId(AUDIO_PRESET_ID)
                                                  .withKey(audioOutputKey)
                                                  .withSegmentDuration(SEGMENT_DURATION),
                             new CreateJobOutput().withPresetId(VIDEO_PRESET_ID)
                                                  .withKey(videoOutputKey)
                                                  .withSegmentDuration(SEGMENT_DURATION))
                .withPlaylists(new CreateJobPlaylist().withName(playlistName)
                                                      .withFormat(PLAYLIST_FORMAT)
                                                      .withOutputKeys(audioOutputKey, videoOutputKey));
        } else {
            System.out.println("SKIPPING file: " + contentId +
                               " (it does not have a .mp3 or .mp4 extension");
            return false; // Skip to next content item
        }

        if (dryRun) {
            System.out.println("Transcoding Job created for: " + contentId +
                               "; current status: none (dryrun mode).");
        } else { // Not a dry run, create the job
            CreateJobResult createJobResult = transcoderClient.createJob(createJobRequest);
            System.out.println("Transcoding Job created for: " + contentId +
                               "; current status: " + createJobResult.getJob().getStatus());
            waitMs(500); // Wait half a second to limit create job requests to 2 per second
        }

        if (verbose) {
            System.out.println("\t Job Details: " + createJobRequest.toString());
        }

        return true;
    }

    /**
     * Causes the current thread to wait for a given number of milliseconds.
     *
     * @param milliseconds - the number of milliseconds to wait
     */
    public static void waitMs(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            // Return on interruption
        }
    }

    /**
     * Manages the command line execution, including all command line parameters
     *
     * @param args - command line arguments
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        cmdOptions = new Options();

        Option bucketNameOption =
            new Option("b", "bucketname", true,
                       "the name of the bucket in which content to be transcoded resides");
        bucketNameOption.setRequired(false);
        cmdOptions.addOption(bucketNameOption);

        Option filePathOption =
            new Option("f", "file", true,
                       "the path to a file which has one content ID per line");
        filePathOption.setRequired(false);
        cmdOptions.addOption(filePathOption);

        // AWS Profile setup: http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html
        Option awsProfileOption =
            new Option("c", "credentials-profile", true,
                       "the AWS CLI credentials profile to use for connection to AWS");
        awsProfileOption.setRequired(true);
        cmdOptions.addOption(awsProfileOption);

        Option pipelineIdOption =
            new Option("p", "pipeline", true,
                       "the ID of the Elastic Transcoder Pipeline in which Jobs will be created");
        pipelineIdOption.setRequired(true);
        cmdOptions.addOption(pipelineIdOption);

        Option verboseOption =
            new Option("v", "verbose", false,
                       "provides additional detail in output");
        verboseOption.setRequired(false);
        cmdOptions.addOption(verboseOption);

        Option dryRunOption =
            new Option("d", "dryrun", false,
                       "indicates that this is a dry-run, no jobs should be created");
        dryRunOption.setRequired(false);
        cmdOptions.addOption(dryRunOption);

        CommandLine cmd = null;
        try {
            CommandLineParser parser = new PosixParser();
            cmd = parser.parse(cmdOptions, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            usage();
        }

        String bucketName = null;
        String filePath = null;
        if (cmd.hasOption("b")) {
            bucketName = cmd.getOptionValue("b");
        } else if (cmd.hasOption("f")) {
            filePath = cmd.getOptionValue("f");
        } else {
            System.out.println("Either bucket name (-b) or file (-f) must be provided");
            usage();
        }

        String awsProfile = cmd.getOptionValue("c");
        String profileId = cmd.getOptionValue("p");

        boolean verbose = false;
        if (cmd.hasOption("v")) {
            verbose = true;
        }

        boolean dryRun = false;
        if (cmd.hasOption("d")) {
            dryRun = true;
        }

        TranscodingJobGenerator generator =
            new TranscodingJobGenerator(awsProfile, bucketName, filePath, profileId, verbose, dryRun);
        generator.run();
    }

    /**
     * Called when the command line arguments are not valid. Prints information
     * about how the tool should be used and exits.
     */
    private static void usage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Running the Transcoding Job Generator", cmdOptions);
        System.exit(1);
    }

}