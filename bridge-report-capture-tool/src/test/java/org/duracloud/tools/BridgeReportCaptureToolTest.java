/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 *     http://duracloud.org/license/
 */
package org.duracloud.tools;

import static org.duracloud.tools.BridgeReportCaptureTool.BRIDGE_PASSWORD_PROP;
import static org.duracloud.tools.BridgeReportCaptureTool.BRIDGE_URL_PROP;
import static org.duracloud.tools.BridgeReportCaptureTool.BRIDGE_USERNAME_PROP;
import static org.duracloud.tools.BridgeReportCaptureTool.S3_ACCESS_KEY_PROP;
import static org.duracloud.tools.BridgeReportCaptureTool.S3_BUCKET_NAME_PROP;
import static org.duracloud.tools.BridgeReportCaptureTool.S3_SECRET_KEY_PROP;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

/**
 * Performs tests on the bridge report capture tool.
 *
 * @author Bill Branan
 * Date: 2/1/2017
 */
public class BridgeReportCaptureToolTest {

    /**
     * Tests the reading and writing of properties to and from a file
     *
     * @throws Exception
     */
    @Test
    public void testPropsWriteRead() throws Exception {
        File propsFile = new File(System.getProperty("java.io.tmpdir"),
                                  "test-bridge-props-file.properties");

        try {
            String bridgeUrlValue = "bridge-url";
            String bridgeUserValue = "bridge-username";
            String bridgePassValue = "bridge-password";
            String s3AccessValue = "s3-access-key";
            String s3SecretValue = "s3-secret-key";
            String s3BucketValue = "s3-bucket-name";

            Properties writeProps = new Properties();
            writeProps.put(BRIDGE_URL_PROP, bridgeUrlValue);
            writeProps.put(BRIDGE_USERNAME_PROP, bridgeUserValue);
            writeProps.put(BRIDGE_PASSWORD_PROP, bridgePassValue);
            writeProps.put(S3_ACCESS_KEY_PROP, s3AccessValue);
            writeProps.put(S3_SECRET_KEY_PROP, s3SecretValue);
            writeProps.put(S3_BUCKET_NAME_PROP, s3BucketValue);

            BridgeReportCaptureTool.writeProps(propsFile.getAbsolutePath(), writeProps);

            Properties readProps =
                BridgeReportCaptureTool.readProps(propsFile.getAbsolutePath());

            assertEquals(6, readProps.size());
            assertEquals(bridgeUrlValue, readProps.get((BRIDGE_URL_PROP)));
            assertEquals(bridgeUserValue, readProps.get((BRIDGE_USERNAME_PROP)));
            assertEquals(bridgePassValue, readProps.get((BRIDGE_PASSWORD_PROP)));
            assertEquals(s3AccessValue, readProps.get((S3_ACCESS_KEY_PROP)));
            assertEquals(s3SecretValue, readProps.get((S3_SECRET_KEY_PROP)));
            assertEquals(s3BucketValue, readProps.get((S3_BUCKET_NAME_PROP)));
        } finally {
            if (propsFile.exists()) {
                FileUtils.forceDelete(propsFile);
            }
        }

    }

}
