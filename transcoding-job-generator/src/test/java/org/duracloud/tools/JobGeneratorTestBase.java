package org.duracloud.tools;

import java.util.List;

import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * @author Bill Branan
 * Date: Aug 24, 2018
 */
public abstract class JobGeneratorTestBase {

    public class SettableObjectListing extends ObjectListing {

        private List<S3ObjectSummary> objectSums;

        public void setObjectSummaries(List<S3ObjectSummary> objectSummaries) {
            this.objectSums = objectSummaries;
        }

        @Override
        public List<S3ObjectSummary> getObjectSummaries() {
            return objectSums;
        }
    }

}
