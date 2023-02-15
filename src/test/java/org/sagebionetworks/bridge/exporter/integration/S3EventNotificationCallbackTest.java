package org.sagebionetworks.bridge.exporter.integration;

import static org.testng.Assert.assertNotSame;

import java.io.File;
import java.nio.charset.StandardCharsets;

import com.google.common.io.Files;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.model.UploadRequest;
import org.sagebionetworks.bridge.rest.model.UploadSession;
import org.sagebionetworks.bridge.rest.model.UploadStatus;
import org.sagebionetworks.bridge.rest.model.UploadValidationStatus;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;

@SuppressWarnings("ConstantConditions")
public class S3EventNotificationCallbackTest {
    private static final String CONTENT_TYPE_TEXT_PLAIN = "text/plain";

    private TestUser user;

    @BeforeMethod
    public void beforeMethod() throws Exception {
        user = TestUserHelper.createAndSignInUser(S3EventNotificationCallbackTest.class, true);
    }

    @AfterMethod
    public void afterMethod() {
        if (user != null) {
            user.signOutAndDeleteUser();
        }
    }

    @Test
    public void test() throws Exception {
        // Create a temp file so that we can use RestUtils.
        String content = "File content doesn't matter for this test";
        File file = File.createTempFile("text", ".txt");
        Files.write(content.getBytes(StandardCharsets.UTF_8), file);

        // Create upload request. , RestUtils defaults to application/zip. We want to overwrite this.
        UploadRequest uploadRequest = RestUtils.makeUploadRequestForFile(file);
        uploadRequest.setContentType(CONTENT_TYPE_TEXT_PLAIN);
        uploadRequest.setEncrypted(false);
        uploadRequest.setZipped(false);

        // Upload.
        ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);
        UploadSession session = usersApi.requestUploadSession(uploadRequest).execute().body();
        String uploadId = session.getId();
        RestUtils.uploadToS3(file, session.getUrl(), CONTENT_TYPE_TEXT_PLAIN);

        // Sleep for a bit to give S3EventNotificationCallback time to finish.
        Thread.sleep(5000);

        // Check that the upload status has moved to a status other than Requested.
        UploadValidationStatus uploadValidationStatus = usersApi.getUploadStatus(uploadId).execute().body();
        assertNotSame(uploadValidationStatus.getStatus(), UploadStatus.REQUESTED);
    }
}
