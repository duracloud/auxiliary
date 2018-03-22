package org.duracloud.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.duracloud.common.model.Credential;
import org.duracloud.common.util.DateUtil;
import org.duracloud.common.util.EncryptionUtil;
import org.duracloud.common.web.RestHttpHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * BridgeReportCaptureTool - Provides a simple way to capture a DuraCloud
 *                           bridge report and store that report in S3.
 *
 * @author: Bill Branan
 * Date: Jan 30, 2017
 */
public class BridgeReportCaptureTool {

    private final Logger log = LoggerFactory.getLogger(BridgeReportCaptureTool.class);

    protected static final String BRIDGE_URL_PROP = "bridge-url";
    protected static final String BRIDGE_USERNAME_PROP = "bridge-username";
    protected static final String BRIDGE_PASSWORD_PROP = "bridge-password";
    protected static final String S3_ACCESS_KEY_PROP = "s3-access-key";
    protected static final String S3_SECRET_KEY_PROP = "s3-secret-key";
    protected static final String S3_BUCKET_NAME_PROP = "s3-bucket-name";

    private static Options cmdOptions;
    private static EncryptionUtil encUtil = new EncryptionUtil();

    /**
     * Kicks off the execution of the tool. Handles retrieving the bridge report and
     * storing that report in S3.
     *
     * @throws IOException
     */
    public void run(Properties props) throws IOException {
        EncryptionUtil enc = new EncryptionUtil();

        // Pull config details from properties, decrypt where needed
        String bridgeUrl = props.getProperty(BRIDGE_URL_PROP);
        String bridgeUsername = enc.decrypt(props.getProperty(BRIDGE_USERNAME_PROP));
        String bridgePassword = enc.decrypt(props.getProperty(BRIDGE_PASSWORD_PROP));
        String s3AccessKey = enc.decrypt(props.getProperty(S3_ACCESS_KEY_PROP));
        String s3SecretKey = enc.decrypt(props.getProperty(S3_SECRET_KEY_PROP));
        String s3BucketName = props.getProperty(S3_BUCKET_NAME_PROP);

        if (null == bridgeUrl ||
            null == bridgeUsername ||
            null == bridgePassword ||
            null == s3AccessKey ||
            null == s3SecretKey ||
            null == s3BucketName) {
            throw new RuntimeException("Properties file is incomplete.");
        }

        Credential bridgeCredential = new Credential(bridgeUsername, bridgePassword);
        RestHttpHelper httpHelper = new RestHttpHelper(bridgeCredential);

        BasicAWSCredentials awsCredentials =
            new BasicAWSCredentials(s3AccessKey, s3SecretKey);
        AmazonS3Client client = new AmazonS3Client(awsCredentials);

        InputStream bridgeReport;
        try {
            RestHttpHelper.HttpResponse bridgeReportResponse = httpHelper.get(bridgeUrl);
            bridgeReport = bridgeReportResponse.getResponseStream();
            if (null == bridgeReport) {
                throw new RuntimeException("Call to bridge to request report failed.");
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to retrieve bridge report due to: " +
                                       e.getMessage());
        }

        String reportName = "dcv-snapshot-report-" + DateUtil.nowShort() + ".json";

        try {
            client.putObject(s3BucketName, reportName, bridgeReport, null);
        } catch (AmazonClientException e) {
            throw new RuntimeException("Failed to write bridge report to S3 due to: " +
                                       e.getMessage());
        }

        log.info("Successfully wrote bridge report " + reportName +
                 " to S3 bucket " + s3BucketName);
    }

    /*
     * Builds parser for reading command line arguments
     */
    private static void createArgsParser() {
        cmdOptions = new Options();

        Option propsFileOption =
            new Option("f", "props-file", true,
                       "the full path to the properties file where tool configuration params reside");
        propsFileOption.setRequired(true);
        cmdOptions.addOption(propsFileOption);

        Option bridgeUrlOption =
            new Option("r", "bridge-url", true,
                       "the URL at which the bridge app can be found");
        bridgeUrlOption.setRequired(false);
        cmdOptions.addOption(bridgeUrlOption);

        Option usernameOption =
            new Option("u", "bridge-username", true,
                       "the username necessary to read from the bridge");
        usernameOption.setRequired(false);
        cmdOptions.addOption(usernameOption);

        Option passwordOption =
            new Option("p", "bridge-password", true,
                       "the password necessary to read from the bridge");
        passwordOption.setRequired(false);
        cmdOptions.addOption(passwordOption);

        Option s3AccessKeyOption =
            new Option("a", "s3-access-key", true, "the AWS access key ID");
        s3AccessKeyOption.setRequired(false);
        cmdOptions.addOption(s3AccessKeyOption);

        Option s3SecretKeyOption =
            new Option("s", "s3-secret-key", true, "the AWS secret access key");
        s3SecretKeyOption.setRequired(false);
        cmdOptions.addOption(s3SecretKeyOption);

        Option s3BucketNameOption =
            new Option("b", "s3-bucket-name", true, "the S3 bucket name");
        s3BucketNameOption.setRequired(false);
        cmdOptions.addOption(s3BucketNameOption);
    }

    /*
     * Parses command line arguments
     */
    private static CommandLine parseArgs(String[] args) {
        try {
            CommandLineParser parser = new PosixParser();
            return parser.parse(cmdOptions, args);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /*
     * Reads tool configuration properties from a file
     */
    protected static Properties readProps(String propsFilePath) throws IOException {
        File propsFile = new File(propsFilePath);
        if (!propsFile.exists()) {
            throw new FileNotFoundException("No file exists at path: " + propsFilePath);
        }

        Properties props = new Properties();
        try (Reader propsReader = new InputStreamReader(new FileInputStream(propsFile),
                                                        StandardCharsets.UTF_8)) {
            props.load(propsReader);
        }
        return props;
    }

    /*
     * Writes tool configuration properties to a file
     */
    protected static void writeProps(String propsFilePath, Properties props)
        throws IOException {
        File propsFile = new File(propsFilePath);
        try (Writer propsFileWriter = new OutputStreamWriter(new FileOutputStream(propsFile),
                                                             StandardCharsets.UTF_8)) {
            props.store(propsFileWriter, null);
        }
    }

    /**
     * Manages the command line execution, including all command line parameters
     *
     * @param args - command line arguments
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        try {
            createArgsParser();

            if (args.length == 2) { // Capture report mode
                CommandLine cmd = parseArgs(args);
                String propsFilePath = cmd.getOptionValue("f");
                Properties props = readProps(propsFilePath);
                BridgeReportCaptureTool tool = new BridgeReportCaptureTool();
                tool.run(props);
            } else if (args.length > 2) { // Write properties mode
                CommandLine cmd = parseArgs(args);
                String propsFilePath = cmd.getOptionValue("f");
                String bridgeUrl = cmd.getOptionValue("r");
                String bridgeUsername = cmd.getOptionValue("u");
                String bridgePassword = cmd.getOptionValue("p");
                String s3AccessKey = cmd.getOptionValue("a");
                String s3SecretKey = cmd.getOptionValue("s");
                String s3BucketName = cmd.getOptionValue("b");

                if (null == propsFilePath ||
                    null == bridgeUrl ||
                    null == bridgeUsername ||
                    null == bridgePassword ||
                    null == s3AccessKey ||
                    null == s3SecretKey ||
                    null == s3BucketName) {
                    System.out.println("To write properties file, " +
                                       "all parameters are required.");
                    usage();
                }

                EncryptionUtil enc = new EncryptionUtil();

                Properties props = new Properties();
                props.put(BRIDGE_URL_PROP, bridgeUrl);
                props.put(BRIDGE_USERNAME_PROP, enc.encrypt(bridgeUsername));
                props.put(BRIDGE_PASSWORD_PROP, enc.encrypt(bridgePassword));
                props.put(S3_ACCESS_KEY_PROP, enc.encrypt(s3AccessKey));
                props.put(S3_SECRET_KEY_PROP, enc.encrypt(s3SecretKey));
                props.put(S3_BUCKET_NAME_PROP, s3BucketName);

                writeProps(propsFilePath, props);
                System.out.println("Successfully wrote properties file to: " +
                                   propsFilePath);
            } else {
                usage();
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            usage();
        }
    }

    /**
     * Called when the command line arguments are not valid. Prints information
     * about how the tool should be used and exits.
     */
    private static void usage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("The Bridge Report Capture Tool can be run in one of two " +
                            "modes. The first takes all parameters and creates an " +
                            "encrypted properties file; the second takes only the " +
                            "-f parameter to specify the properties file. It is the " +
                            "second mode in which calls are made to the bridge app to " +
                            "retrieve a report and store that report in S3.", cmdOptions);
        System.exit(1);
    }

}