package org.sagebionetworks.bridge.exporter.integration;

import static org.testng.Assert.assertEquals;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3Client;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.UploadStatus;
import org.sagebionetworks.bridge.rest.model.UploadValidationStatus;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.SqsHelper;
import org.sagebionetworks.bridge.user.TestUserHelper;

@SuppressWarnings({ "deprecation", "unchecked" })
public class UploadRedriveTest {
    private static final String WORKER_ID = "UploadRedriveWorker";

    private static Config config;
    private static S3Helper s3Helper;
    private static SqsHelper sqsHelper;
    private static String workerSqsUrl;
    private static Table ddbWorkerLogTable;
    private static TestUserHelper.TestUser developer;

    private UploadValidationStatus uploadValidationStatus;
    private TestUserHelper.TestUser user;

    @BeforeClass
    public static void setupTest() throws Exception {
        // AWS credentials.
        config = TestUtils.loadConfig();
        AWSCredentialsProvider awsCredentialsProvider = TestUtils.getAwsCredentialsForConfig(config);

        // DDB tables.
        DynamoDB ddbClient = TestUtils.getDdbClient(awsCredentialsProvider);
        ddbWorkerLogTable = TestUtils.getDdbTable(config, ddbClient, "WorkerLog");

        // S3.
        AmazonS3Client s3Client = new AmazonS3Client(awsCredentialsProvider);
        s3Helper = new S3Helper();
        s3Helper.setS3Client(s3Client);

        // SQS.
        workerSqsUrl = config.get("worker.request.sqs.queue.url");
        sqsHelper = TestUtils.getSqsHelper(awsCredentialsProvider);

        // Ensure we have the schemas we need for this test.
        developer = TestUserHelper.createAndSignInUser(UploadRedriveTest.class, false, Role.DEVELOPER);
        TestUtils.ensureSchemas(developer);
    }

    @BeforeMethod
    public void setupUser() throws Exception {
        // Create user.
        user = TestUserHelper.createAndSignInUser(UploadRedriveTest.class, true);
        ForConsentedUsersApi userApi = user.getClient(ForConsentedUsersApi.class);

        // Set user's sharing status, because this is one of the few pieces of upload metadata we can easily control.
        StudyParticipant participant = userApi.getUsersParticipantRecord().execute().body();
        participant.setSharingScope(SharingScope.ALL_QUALIFIED_RESEARCHERS);
        userApi.updateUsersParticipantRecord(participant).execute();

        // Upload. Verify that upload succeeded and is tagged with ALL_QUALIFIED_RESEARCHERS.
        uploadValidationStatus = TestUtils.upload(user);
        assertEquals(uploadValidationStatus.getStatus(), UploadStatus.SUCCEEDED);
        assertEquals(uploadValidationStatus.getRecord().getUserSharingScope(), SharingScope.ALL_QUALIFIED_RESEARCHERS);
    }

    @AfterMethod
    public void deleteUser() throws Exception {
        if (user != null) {
            user.signOutAndDeleteUser();
        }
    }

    @AfterClass
    public static void deleteDeveloper() throws Exception {
        if (developer != null) {
            developer.signOutAndDeleteUser();
        }
    }

    @Test
    public void redriveUploadId() throws Exception {
        executeTest(uploadValidationStatus.getId(), "upload_id");
        validateUpload(uploadValidationStatus.getId());
    }

    @Test
    public void redriveRecordId() throws Exception {
        executeTest(uploadValidationStatus.getRecord().getId(), "record_id");
        validateUpload(uploadValidationStatus.getId());
    }

    private void executeTest(String idToRedrive, String redriveType) throws Exception {
        // Change the user's sharing status to sponsors_and_partners, so we can verify this change when we redrive.
        ForConsentedUsersApi userApi = user.getClient(ForConsentedUsersApi.class);
        StudyParticipant participant = userApi.getUsersParticipantRecord().execute().body();
        participant.setSharingScope(SharingScope.SPONSORS_AND_PARTNERS);
        userApi.updateUsersParticipantRecord(participant).execute();

        // Get S3 bucket name from config and generate filename.
        String s3Bucket = config.get("backfill.bucket");
        String s3Key = "redrive-integ-test-" + DateTime.now();

        // Write ID list to S3.
        s3Helper.writeLinesToS3(s3Bucket, s3Key, ImmutableList.of(idToRedrive));

        // We need to know the previous finish time so we can determine when the worker is finished.
        long previousFinishTime = TestUtils.getWorkerLastFinishedTime(ddbWorkerLogTable, WORKER_ID);

        // Create request.
        String requestText = "{\n" +
                "   \"service\":\"UploadRedriveWorker\",\n" +
                "   \"body\":{\n" +
                "       \"s3Bucket\":\"" + s3Bucket + "\",\n" +
                "       \"s3Key\":\"" + s3Key + "\",\n" +
                "       \"redriveType\":\"" + redriveType + "\"\n" +
                "   }\n" +
                "}";
        JsonNode requestNode = DefaultObjectMapper.INSTANCE.readTree(requestText);
        sqsHelper.sendMessageAsJson(workerSqsUrl, requestNode, 0);

        // Wait until the worker is finished.
        TestUtils.pollWorkerLog(ddbWorkerLogTable, WORKER_ID, previousFinishTime);
    }

    private void validateUpload(String uploadId) throws Exception {
        UploadValidationStatus uploadValidationStatus = user.getClient(ForConsentedUsersApi.class)
                .getUploadStatus(uploadId).execute().body();
        assertEquals(uploadValidationStatus.getStatus(), UploadStatus.SUCCEEDED);
        assertEquals(uploadValidationStatus.getRecord().getUserSharingScope(), SharingScope.SPONSORS_AND_PARTNERS);
    }
}
