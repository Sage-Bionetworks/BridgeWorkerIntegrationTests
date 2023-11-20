package org.sagebionetworks.bridge.exporter.integration;

import static org.sagebionetworks.bridge.rest.model.PerformanceOrder.SEQUENTIAL;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_ID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.apache.http.util.EntityUtils;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseStsCredentialsProvider;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseNotFoundException;
import org.sagebionetworks.repo.model.annotation.v2.Annotations;
import org.sagebionetworks.repo.model.annotation.v2.AnnotationsValue;
import org.sagebionetworks.repo.model.annotation.v2.AnnotationsValueType;
import org.sagebionetworks.repo.model.sts.StsPermission;
import org.sagebionetworks.repo.model.table.Row;
import org.sagebionetworks.repo.model.table.RowSet;
import org.sagebionetworks.repo.model.table.SelectColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.crypto.BcCmsEncryptor;
import org.sagebionetworks.bridge.crypto.PemUtils;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.api.AppConfigsApi;
import org.sagebionetworks.bridge.rest.api.AssessmentsApi;
import org.sagebionetworks.bridge.rest.api.DemographicsApi;
import org.sagebionetworks.bridge.rest.api.ForAdminsApi;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForDevelopersApi;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.api.InternalApi;
import org.sagebionetworks.bridge.rest.api.ParticipantsApi;
import org.sagebionetworks.bridge.rest.api.SchedulesV2Api;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.api.UploadsApi;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.Assessment;
import org.sagebionetworks.bridge.rest.model.AssessmentConfig;
import org.sagebionetworks.bridge.rest.model.AssessmentReference2;
import org.sagebionetworks.bridge.rest.model.ClientInfo;
import org.sagebionetworks.bridge.rest.model.Demographic;
import org.sagebionetworks.bridge.rest.model.DemographicUser;
import org.sagebionetworks.bridge.rest.model.DemographicUserAssessment;
import org.sagebionetworks.bridge.rest.model.DemographicUserAssessmentAnswer;
import org.sagebionetworks.bridge.rest.model.DemographicUserAssessmentAnswerCollection;
import org.sagebionetworks.bridge.rest.model.DemographicUserResponse;
import org.sagebionetworks.bridge.rest.model.DemographicValuesEnumValidationRules;
import org.sagebionetworks.bridge.rest.model.DemographicValuesNumberRangeValidationRules;
import org.sagebionetworks.bridge.rest.model.DemographicValuesValidationConfig;
import org.sagebionetworks.bridge.rest.model.DemographicValuesValidationConfig.ValidationTypeEnum;
import org.sagebionetworks.bridge.rest.model.Enrollment;
import org.sagebionetworks.bridge.rest.model.ExportedRecordInfo;
import org.sagebionetworks.bridge.rest.model.Exporter3Configuration;
import org.sagebionetworks.bridge.rest.model.ExporterSubscriptionRequest;
import org.sagebionetworks.bridge.rest.model.ExporterSubscriptionResult;
import org.sagebionetworks.bridge.rest.model.HealthDataRecordEx3;
import org.sagebionetworks.bridge.rest.model.ParticipantVersion;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.Schedule2;
import org.sagebionetworks.bridge.rest.model.Session;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.rest.model.SharingScopeForm;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.TimeWindow;
import org.sagebionetworks.bridge.rest.model.Timeline;
import org.sagebionetworks.bridge.rest.model.UploadRequest;
import org.sagebionetworks.bridge.rest.model.UploadSession;
import org.sagebionetworks.bridge.rest.model.UploadTableJobResult;
import org.sagebionetworks.bridge.rest.model.UploadTableJobStatus;
import org.sagebionetworks.bridge.rest.model.UploadTableRow;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.SqsHelper;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;
import org.sagebionetworks.bridge.util.IntegTestUtils;

@SuppressWarnings({ "ConstantConditions", "deprecation", "OptionalGetWithoutIsPresent" })
public class Exporter3Test {
    private static final Logger LOG = LoggerFactory.getLogger(Exporter3Test.class);

    private static final String CONFIG_KEY_BACKFILL_BUCKET = "backfill.bucket";
    private static final String CONFIG_KEY_RAW_DATA_BUCKET = "health.data.bucket.raw";
    private static final String CONTENT_TYPE_TEXT_PLAIN = "text/plain";
    private static final String CUSTOM_METADATA_KEY = "custom-metadata-key";
    private static final String CUSTOM_METADATA_KEY_SANITIZED = "custom_metadata_key";
    private static final String CUSTOM_METADATA_VALUE = "custom-metadata-value";
    private static final String DATA_GROUP_SDK_INT_1 = "sdk-int-1";
    private static final String DATA_GROUP_TEST_USER = "test_user";
    private static final String START_DATE_STR = "2015-04-10T10:40:34.000-07:00";
    private static final String END_DATE_STR = "2015-04-10T18:19:51.000-07:00";
    private static final String DEVICE_INFO = "Integ Test Device Info";
    private static final String EXTERNAL_ID = "external-id-1";
    private static final String FILENAME_METADATA_JSON = "metadata.json";
    private static final String FILENAME_ASSESSMENT_RESULT_JSON = "assessmentResult.json";
    private static final String FRAMEWORK_IDENTIFIER_OPEN_BRIDGE_SURVEY = "health.bridgedigital.assessment";
    private static final String STUDY_ID = "study1";
    private static final String STUDY_2_ID = "study2";
    private static final byte[] UPLOAD_CONTENT = "This is the upload content".getBytes(StandardCharsets.UTF_8);
    private static final String WORKER_ID_BACKFILL_PARTICIPANTS = "BackfillParticipantVersionsWorker";
    private static final String WORKER_ID_REDRIVE_PARTICIPANTS = "RedriveParticipantVersionsWorker";
    private static final String DEMOGRAPHICS_VALIDATION_APP_CONFIG_PREFIX = "bridge-validation-demographics-values-";
    private static final String ENUM_VALIDATION_CATEGORY = "enumValidationCategory";
    private static final String NUMBER_VALIDATION_CATEGORY = "numberValidationCategory";
    private static final String APP_CONFIG_ELEMENT_ID_ENUM_VALIDATION = DEMOGRAPHICS_VALIDATION_APP_CONFIG_PREFIX + ENUM_VALIDATION_CATEGORY;
    private static final String APP_CONFIG_ELEMENT_ID_NUMBER_VALIDATION = DEMOGRAPHICS_VALIDATION_APP_CONFIG_PREFIX + NUMBER_VALIDATION_CATEGORY;
    private static final String[] APP_CONFIG_ELEMENTS_TO_DELETE = {APP_CONFIG_ELEMENT_ID_ENUM_VALIDATION, APP_CONFIG_ELEMENT_ID_NUMBER_VALIDATION};
    private static final String INVALID_ENUM_VALUE = "invalid enum value";
    private static final String INVALID_NUMBER_VALUE_GREATER_THAN_MAX = "invalid number (larger than max)";

    private static final String APP_NAME_FOR_USER = "app-name-for-user";
    private static final ClientInfo CLIENT_INFO_FOR_USER = new ClientInfo().appName(APP_NAME_FOR_USER);

    private static final ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool();

    // Simplified assessment config with only the relevant fields.
    private static final String ASSESSMENT_CONFIG = "{\n" +
            "  \"type\": \"assessment\",\n" +
            "  \"identifier\":\"xhcsds\",\n" +
            "  \"steps\": [\n" +
            "    {\n" +
            "      \"type\": \"choiceQuestion\",\n" +
            "      \"identifier\": \"choiceQ1\",\n" +
            "      \"title\": \"Choose which question to answer\",\n" +
            "      \"baseType\": \"integer\",\n" +
            "      \"singleChoice\": true,\n" +
            "      \"choices\": [\n" +
            "        {\n" +
            "          \"value\": 1,\n" +
            "          \"text\": \"Enter some text\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"value\": 2,\n" +
            "          \"text\": \"Birth year\"\n" +
            "        }\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"type\": \"simpleQuestion\",\n" +
            "      \"identifier\": \"simpleQ1\",\n" +
            "      \"title\": \"Enter some text\",\n" +
            "      \"inputItem\": {\n" +
            "        \"type\": \"string\",\n" +
            "        \"placeholder\": \"I like cake\"\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    // Simplified metadata.json file with only the relevant fields.
    private static final String METADATA_JSON_CONTENT = "{\n" +
            "   \"deviceInfo\":\"" + DEVICE_INFO + "\",\n" +
            "   \"startDate\":\"" + START_DATE_STR + "\",\n" +
            "   \"endDate\":\"" + END_DATE_STR +  "\"\n" +
            "}";

    // Simplified assessmentResults.json file with only the relevant fields.
    private static final String ASSESSMENT_RESULTS_JSON_CONTENT = "{\n" +
            "   \"type\": \"assessment\",\n" +
            "   \"identifier\":\"xhcsds\",\n" +
            "   \"stepHistory\":[\n" +
            "      {\n" +
            "         \"type\":\"answer\",\n" +
            "         \"identifier\":\"choiceQ1\",\n" +
            "         \"answerType\":{\n" +
            "            \"type\":\"integer\"\n" +
            "         },\n" +
            "         \"value\":1\n" +
            "      },\n" +
            "      {\n" +
            "         \"type\":\"answer\",\n" +
            "         \"identifier\":\"simpleQ1\",\n" +
            "         \"answerType\":{\n" +
            "            \"type\":\"string\"\n" +
            "         },\n" +
            "         \"value\":\"test text\"\n" +
            "      }\n" +
            "   ]\n" +
            "}";

    private static final Map<String, String> SURVEY_UPLOAD_CONTENT_BY_FILENAME = ImmutableMap.of(
            FILENAME_METADATA_JSON, METADATA_JSON_CONTENT,
            FILENAME_ASSESSMENT_RESULT_JSON, ASSESSMENT_RESULTS_JSON_CONTENT);

    private static final String[] SURVEY_UPLOAD_TABLE_COLUMNS = {
            "clientInfo",
            "dataGroups",
            "deviceInfo",
            "endDate",
            "externalId",
            "sessionGuid",
            "sessionName",
            "sessionStartEventId",
            "startDate",
            "userAgent",
            "choiceQ1",
            "simpleQ1",
    };

    private static TestUser adminDeveloperWorker;
    private static TestUser researcher;
    private static TestUser studyDesigner;
    private static String backfillBucket;
    private static Table ddbWorkerLogTable;
    private static Exporter3Configuration ex3Config;
    private static Exporter3Configuration ex3ConfigForStudy;
    private static Exporter3Configuration ex3ConfigForStudy2;
    private static String rawDataBucket;
    private static S3Helper s3Helper;
    private static AmazonSNS snsClient;
    private static AmazonSQS sqsClient;
    private static SqsHelper sqsHelper;
    private static SynapseClient synapseClient;
    private static SynapseHelper synapseHelper;
    private static String testQueueArn;
    private static String testQueueUrl;
    private static String workerSqsUrl;

    private TestUser user;
    private Schedule2 schedule;
    private List<String> subscriptionArnList;
    private List<String> tableRowsToDelete;
    private Assessment assessment;

    @BeforeClass
    public static void beforeClass() throws Exception {
        Config config = TestUtils.loadConfig();
        backfillBucket = config.get(CONFIG_KEY_BACKFILL_BUCKET);
        rawDataBucket = config.get(CONFIG_KEY_RAW_DATA_BUCKET);

        // Set up AWS clients.
        AWSCredentialsProvider awsCredentialsProvider = TestUtils.getAwsCredentialsForConfig(config);

        DynamoDB ddbClient = TestUtils.getDdbClient(awsCredentialsProvider);
        ddbWorkerLogTable = TestUtils.getDdbTable(config, ddbClient, "WorkerLog");

        AmazonS3Client s3Client = new AmazonS3Client(awsCredentialsProvider);
        s3Helper = new S3Helper();
        s3Helper.setS3Client(s3Client);

        snsClient = AmazonSNSClientBuilder.standard().withCredentials(awsCredentialsProvider).build();

        workerSqsUrl = config.get("worker.request.sqs.queue.url");
        sqsClient = AmazonSQSClientBuilder.standard().withCredentials(awsCredentialsProvider).build();
        sqsHelper = TestUtils.getSqsHelper(awsCredentialsProvider);

        testQueueArn = config.get("integ.test.queue.arn");
        testQueueUrl = config.get("integ.test.queue.url");

        // Set up SynapseClient.
        synapseClient = TestUtils.getSynapseClient(config);

        // Set up SynapseHelper so that the Synapse calls in our integ tests have retries. Instead of 5 minute
        // exponential backoff, use 15 tries with a 1 second delay each (which is how the previous version of the test
        // worked).
        int[] backoffPlan = new int[15];
        Arrays.fill(backoffPlan, 1);
        synapseHelper = new SynapseHelper();
        synapseHelper.setSynapseClient(synapseClient);
        synapseHelper.setAsyncGetBackoffPlan(backoffPlan);

        // Create admin account.
        adminDeveloperWorker = TestUserHelper.createAndSignInUser(Exporter3Test.class, false, Role.ADMIN,
                Role.DEVELOPER, Role.WORKER);

        // Create researcher account
        researcher = TestUserHelper.createAndSignInUser(Exporter3Test.class, false, Role.RESEARCHER);

        // Create study designer account
        studyDesigner = TestUserHelper.createAndSignInUser(Exporter3Test.class, false, Role.STUDY_DESIGNER);

        // Wipe the Exporter 3 Config and re-create it.
        deleteEx3Resources();

        // Clear queue. Note that PurgeQueue can only be called at most once every 60 seconds, or it will throw an
        // exception.
        PurgeQueueRequest purgeQueueRequest = new PurgeQueueRequest(testQueueUrl);
        sqsClient.purgeQueue(purgeQueueRequest);

        // Wait one second to ensure the queue is cleared.
        Thread.sleep(1000);

        // Init Exporter 3.
        ForAdminsApi adminsApi = adminDeveloperWorker.getClient(ForAdminsApi.class);
        ex3Config = adminsApi.initExporter3().execute().body();
        ex3ConfigForStudy = adminsApi.initExporter3ForStudy(STUDY_ID).execute().body();
        ex3ConfigForStudy2 = adminsApi.initExporter3ForStudy(STUDY_2_ID).execute().body();
    }

    @BeforeMethod
    public void before() throws Exception {
        // Note: Consent also enrolls the participant in study1, but we want to add an external ID.
        user = new TestUserHelper.Builder(Exporter3Test.class).withClientInfo(CLIENT_INFO_FOR_USER)
                .withConsentUser(true).createAndSignInUser();

        Enrollment enrollment1 = new Enrollment().userId(user.getUserId()).externalId(EXTERNAL_ID + ":" + STUDY_ID);
        adminDeveloperWorker.getClient(StudiesApi.class).enrollParticipant(STUDY_ID, enrollment1).execute();

        adminDeveloperWorker.getClient(StudiesApi.class)
                .enrollParticipant(STUDY_2_ID, new Enrollment().userId(user.getUserId())).execute();

        subscriptionArnList = new ArrayList<>();

        tableRowsToDelete = new ArrayList<>();
    }

    @AfterMethod
    public void after() throws Exception {
        for (String subscriptionArn : subscriptionArnList) {
            snsClient.unsubscribe(subscriptionArn);
        }

        // Delete upload table rows.
        TestUser admin = TestUserHelper.getSignedInAdmin();
        for (String tableRowRecordId : tableRowsToDelete) {
            admin.getClient(UploadsApi.class).deleteUploadTableRowForSuperadmin(TEST_APP_ID, STUDY_ID,
                    tableRowRecordId).execute();
        }

        // Delete participant version and user.
        if (user != null) {
            adminDeveloperWorker.getClient(InternalApi.class).deleteAllParticipantVersionsForUser(user.getUserId())
                    .execute();
            user.signOutAndDeleteUser();
        }
        if (schedule != null) {
            SchedulesV2Api schedulesApi = admin.getClient(SchedulesV2Api.class);
            schedulesApi.deleteSchedule(schedule.getGuid()).execute();
        }
        if (assessment != null) {
            AssessmentsApi assessmentsApi = admin.getClient(AssessmentsApi.class);
            assessmentsApi.deleteAssessment(assessment.getGuid(), true).execute();
        }
    }

    @AfterClass
    public static void afterClass() throws Exception {
        // Clean up Synapse resources.
        deleteEx3Resources();
        // Clean up app config elements
        AppConfigsApi appConfigsApi = adminDeveloperWorker.getClient(AppConfigsApi.class);
        for (String appConfigElementId : APP_CONFIG_ELEMENTS_TO_DELETE) {
            appConfigsApi.deleteAllAppConfigElementRevisions(appConfigElementId, true).execute();
        }

        if (adminDeveloperWorker != null) {
            adminDeveloperWorker.signOutAndDeleteUser();
        }
        if (researcher != null) {
            researcher.signOutAndDeleteUser();
        }
        if (studyDesigner != null) {
            studyDesigner.signOutAndDeleteUser();
        }
    }

    private static void deleteEx3Resources() throws IOException {
        // Delete for app.
        ForAdminsApi adminsApi = adminDeveloperWorker.getClient(ForAdminsApi.class);
        App app = adminsApi.getUsersApp().execute().body();
        Exporter3Configuration ex3Config = app.getExporter3Configuration();
        deleteEx3Resources(ex3Config);

        app.setExporter3Configuration(null);
        app.setExporter3Enabled(false);
        adminsApi.updateUsersApp(app).execute();

        // Delete for study.
        StudiesApi studiesApi = adminDeveloperWorker.getClient(StudiesApi.class);
        Study study = studiesApi.getStudy(STUDY_ID).execute().body();
        Exporter3Configuration ex3ConfigForStudy = study.getExporter3Configuration();
        deleteEx3Resources(ex3ConfigForStudy);

        study.setExporter3Configuration(null);
        study.setExporter3Enabled(false);
        studiesApi.updateStudy(STUDY_ID, study).execute();

        // Delete for study2.
        Study study2 = studiesApi.getStudy(STUDY_2_ID).execute().body();
        Exporter3Configuration ex3ConfigForStudy2 = study2.getExporter3Configuration();
        deleteEx3Resources(ex3ConfigForStudy2);

        study2.setExporter3Configuration(null);
        study2.setExporter3Enabled(false);
        studiesApi.updateStudy(STUDY_2_ID, study2).execute();
    }

    private static void deleteEx3Resources(Exporter3Configuration ex3Config) {
        if (ex3Config == null) {
            // Exporter 3 is not configured on this app. We can skip this step.
            return;
        }

        // Delete the project. This automatically deletes the folder too.
        String projectId = ex3Config.getProjectId();
        if (projectId != null) {
            try {
                synapseClient.deleteEntityById(projectId, true);
            } catch (SynapseException ex) {
                LOG.error("Error deleting project " + projectId, ex);
            }
        }

        // Delete the data access team.
        Long dataAccessTeamId = ex3Config.getDataAccessTeamId();
        if (dataAccessTeamId != null) {
            try {
                synapseClient.deleteTeam(String.valueOf(dataAccessTeamId));
            } catch (SynapseException ex) {
                LOG.error("Error deleting team " + dataAccessTeamId, ex);
            }
        }

        // Storage locations are idempotent, so no need to delete that.

        // Delete the Export Notification SNS topic.
        String exportNotificationTopicArn = ex3Config.getExportNotificationTopicArn();
        if (exportNotificationTopicArn != null) {
            try {
                snsClient.deleteTopic(exportNotificationTopicArn);
            } catch (AmazonClientException ex) {
                LOG.error("Error deleting topic " + exportNotificationTopicArn);
            }
        }
    }

    // Note: There are other test cases, for example, if the participant uploads and then turns off sharing before the
    // upload is exported. This might happen if Synapse is down for maintenance.
    // This specific test will test no_sharing and redrives.
    @Test
    public void noSharingAndRedrives() throws Exception {
        // Participants created by TestUserHelper (UserAdminService) are set to no_sharing by default. No need to
        // change the participant here.

        // Upload file to Bridge.
        UploadInfo uploadInfo = uploadFile(UPLOAD_CONTENT, false);
        String filename = uploadInfo.filename;
        String uploadId = uploadInfo.uploadId;

        // Verify upload is NOT exported to Synapse.
        String rawFolderId = ex3Config.getRawDataFolderId();
        String todaysDateString = LocalDate.now(TestUtils.LOCAL_TIME_ZONE).toString();
        String exportedFilename = uploadId + '-' + filename;

        // Depending on the order that TestNG runs the tests, the parent folder might not have been created yet.
        String todayFolderId = getSynapseChildByName(rawFolderId, todaysDateString);
        String unsharedFileId = null;
        if (todayFolderId != null) {
            unsharedFileId = getSynapseChildByName(todayFolderId, exportedFilename);
        }
        assertNull(unsharedFileId);

        // Add a sharing status.
        ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);
        StudyParticipant participant = usersApi.getUsersParticipantRecord(false).execute().body();
        participant.setSharingScope(SharingScope.ALL_QUALIFIED_RESEARCHERS);
        usersApi.updateUsersParticipantRecord(participant).execute();

        // Redrive will look at participant's current sharing scope, which will cause the redriven upload to be
        // exported.
        usersApi.completeUploadSession(uploadId, true, true).execute();
        Thread.sleep(2000);
        todayFolderId = getSynapseChildByName(rawFolderId, todaysDateString);
        String sharedFileId = getSynapseChildByName(todayFolderId, exportedFilename);
        assertNotNull(sharedFileId);

        // Delete the file from Synapse and redrive again. We have a new file entity ID.
        synapseClient.deleteEntityById(sharedFileId, true);
        usersApi.completeUploadSession(uploadId, true, true).execute();
        Thread.sleep(2000);
        String redrivenFileId = getSynapseChildByName(todayFolderId, exportedFilename);
        assertNotNull(redrivenFileId);
        assertNotEquals(redrivenFileId, sharedFileId);

        // Redrive the upload a second time, but don't delete it first. Worker should silently handle this case.
        usersApi.completeUploadSession(uploadId, true, true).execute();
        Thread.sleep(2000);
        String redrivenFileId2 = getSynapseChildByName(todayFolderId, exportedFilename);
        assertNotNull(redrivenFileId2);
        assertEquals(redrivenFileId2, redrivenFileId);
    }

    @Test
    public void backfillParticipantVersion() throws Exception {
        String userId = user.getUserId();

        // Participant version hasn't been created yet, since TestUserHelper creates the user with no_sharing.
        // Temporarily disable Exporter 3 for app. This way, when we flip the user to all_qualified_researchers, it
        // doesn't create a participant version.
        ForAdminsApi adminsApi = adminDeveloperWorker.getClient(ForAdminsApi.class);
        App app = adminsApi.getUsersApp().execute().body();
        app.setExporter3Enabled(false);
        app.setHealthCodeExportEnabled(true);
        adminsApi.updateUsersApp(app).execute();

        // Also, we need the healthcode.
        StudyParticipant participant = adminDeveloperWorker.getClient(ParticipantsApi.class)
                .getParticipantById(userId, false).execute().body();
        String healthCode = participant.getHealthCode();

        // Set the sharing scope.
        user.getClient(ForConsentedUsersApi.class).changeSharingScope(new SharingScopeForm()
                .scope(SharingScope.ALL_QUALIFIED_RESEARCHERS)).execute();

        // Enable Exporter 3.
        app = adminsApi.getUsersApp().execute().body();
        app.setExporter3Enabled(true);
        adminsApi.updateUsersApp(app).execute();

        // There are no participant versions, since Exporter 3 is was disabled when the user was created.
        ForWorkersApi workersApi = adminDeveloperWorker.getClient(ForWorkersApi.class);
        List<ParticipantVersion> participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertTrue(participantVersionList.isEmpty());

        // Backfill participant version using backfill worker. Start by creating the list of participants to redrive
        // (with one participant).
        String filename = "backfill-" + getClass().getSimpleName() + "-" +
                RandomStringUtils.randomAlphabetic(10);
        s3Helper.writeLinesToS3(backfillBucket, filename, ImmutableList.of(healthCode));

        // We need to know the previous finish time so we can determine when the worker is finished.
        long previousFinishTime = TestUtils.getWorkerLastFinishedTime(ddbWorkerLogTable,
                WORKER_ID_BACKFILL_PARTICIPANTS);

        // Create request.
        String requestText = "{\n" +
                "   \"service\":\"" + WORKER_ID_BACKFILL_PARTICIPANTS + "\",\n" +
                "   \"body\":{\n" +
                "       \"appId\":\"" + TEST_APP_ID + "\",\n" +
                "       \"s3Key\":\"" + filename + "\"\n" +
                "   }\n" +
                "}";
        JsonNode requestNode = DefaultObjectMapper.INSTANCE.readTree(requestText);
        sqsHelper.sendMessageAsJson(workerSqsUrl, requestNode, 0);

        // Wait until the worker is finished.
        TestUtils.pollWorkerLog(ddbWorkerLogTable, WORKER_ID_BACKFILL_PARTICIPANTS, previousFinishTime);

        // Also need to wait for export
        Thread.sleep(30000);

        //There is now 1 participant version.
        participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID, userId).execute().body()
                .getItems();
        assertEquals(1, participantVersionList.size());

        // Participant version was exported to app's Synapse project.
        Future<RowSet> rowSetForApp = queryParticipantVersion(ex3Config, healthCode, null);

        // And for study's Synapse project.
        Future<RowSet> rowSetForStudy = queryParticipantVersion(ex3ConfigForStudy, healthCode, null);

        assertEquals(rowSetForApp.get().getRows().size(), 1);
        assertEquals(rowSetForStudy.get().getRows().size(), 1);
    }

    @Test
    public void redriveParticipantVersion() throws Exception {
        String userId = user.getUserId();

        // Participants created by TestUserHelper (UserAdminService) are set to no_sharing by default. Enable sharing
        // so that the test can succeed.
        user.getClient(ForConsentedUsersApi.class).changeSharingScope(new SharingScopeForm()
                .scope(SharingScope.ALL_QUALIFIED_RESEARCHERS)).execute();

        // Get the user's healthcode.
        StudyParticipant participant = adminDeveloperWorker.getClient(ParticipantsApi.class)
                .getParticipantById(userId, false).execute().body();
        String healthCode = participant.getHealthCode();

        // Sleep, to give the worker time to export to Synapse.
        Thread.sleep(5000);

        // There is 1 participant version.
        ForWorkersApi workersApi = adminDeveloperWorker.getClient(ForWorkersApi.class);
        List<ParticipantVersion> participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertEquals(1, participantVersionList.size());

        // Participant version was exported to app's Synapse project.
        Future<RowSet> rowSetForApp = queryParticipantVersion(ex3Config, healthCode, null);

        // And for study's Synapse project.
        Future<RowSet> rowSetForStudy = queryParticipantVersion(ex3ConfigForStudy, healthCode, null);

        assertEquals(rowSetForApp.get().getRows().size(), 1);
        assertEquals(rowSetForStudy.get().getRows().size(), 1);

        // Redrive participant version using redrive worker. Start by creating the list of participants to redrive
        // (with one participant).
        String filename = "redrive-" + getClass().getSimpleName() + "-" +
                RandomStringUtils.randomAlphabetic(10);
        s3Helper.writeLinesToS3(backfillBucket, filename, ImmutableList.of(healthCode));

        // We need to know the previous finish time so we can determine when the worker is finished.
        long previousFinishTime = TestUtils.getWorkerLastFinishedTime(ddbWorkerLogTable,
                WORKER_ID_REDRIVE_PARTICIPANTS);

        // Create request.
        String requestText = "{\n" +
                "   \"service\":\"" + WORKER_ID_REDRIVE_PARTICIPANTS + "\",\n" +
                "   \"body\":{\n" +
                "       \"appId\":\"" + TEST_APP_ID + "\",\n" +
                "       \"s3Key\":\"" + filename + "\"\n" +
                "   }\n" +
                "}";
        JsonNode requestNode = DefaultObjectMapper.INSTANCE.readTree(requestText);
        sqsHelper.sendMessageAsJson(workerSqsUrl, requestNode, 0);

        // Wait until the worker is finished.
        TestUtils.pollWorkerLog(ddbWorkerLogTable, WORKER_ID_REDRIVE_PARTICIPANTS, previousFinishTime);

        //There is still only 1 participant version.
        participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID, userId).execute().body()
                .getItems();
        assertEquals(1, participantVersionList.size());

        // However, it's been exported a second time to the app's Synapse project.
        rowSetForApp = queryParticipantVersion(ex3Config, healthCode, null);

        // And for study's Synapse project.
        rowSetForStudy = queryParticipantVersion(ex3ConfigForStudy, healthCode, null);

        assertEquals(rowSetForApp.get().getRows().size(), 2);
        assertEquals(rowSetForStudy.get().getRows().size(), 2);
    }

    @Test
    public void encryptedUpload() throws Exception {
        // Encrypt the upload content. Get the public key from Bridge.
        ForDevelopersApi developersApi = adminDeveloperWorker.getClient(ForDevelopersApi.class);
        String publicKey = developersApi.getAppPublicCsmKey().execute().body().getPublicKey();
        X509Certificate cert = PemUtils.loadCertificateFromPem(publicKey);
        BcCmsEncryptor encryptor = new BcCmsEncryptor(cert, null);
        byte[] encryptedUploadContent = encryptor.encrypt(UPLOAD_CONTENT);

        testUpload(encryptedUploadContent, true);
    }

    @Test
    public void unencryptedUpload() throws Exception {
        testUpload(UPLOAD_CONTENT, false);
    }
    
    @Test
    public void uploadWithScheduleContext() throws Exception {
        TestUser admin = TestUserHelper.getSignedInAdmin();
        AssessmentsApi assessmentsApi = admin.getClient(AssessmentsApi.class);
        ParticipantsApi participantsApi = admin.getClient(ParticipantsApi.class);
        SchedulesV2Api schedulesApi = admin.getClient(SchedulesV2Api.class);
        StudiesApi studiesApi = admin.getClient(StudiesApi.class);
        UploadsApi uploadsApi = admin.getClient(UploadsApi.class);

        // Get the user's healthcode.
        String userId = user.getUserId();
        StudyParticipant participant = participantsApi.getParticipantById(userId, false).execute().body();
        String healthCode = participant.getHealthCode();

        // Give participant  data group.
        participant.dataGroups(ImmutableList.of(DATA_GROUP_SDK_INT_1));
        participantsApi.updateParticipant(userId, participant).execute();

        // Make Open Bridge survey assessment.
        String assessmentId = getClass().getSimpleName() + "-" + RandomStringUtils.randomAlphabetic(10);

        assessment = new Assessment().title(assessmentId).osName("Universal").ownerId("sage-bionetworks")
                .identifier(assessmentId).frameworkIdentifier(FRAMEWORK_IDENTIFIER_OPEN_BRIDGE_SURVEY)
                .phase(Assessment.PhaseEnum.DRAFT);
        assessment = assessmentsApi.createAssessment(assessment).execute().body();

        schedule = new Schedule2();
        schedule.setName("Test Schedule [Exporter3Test]");
        schedule.setDuration("P1W");

        Session session = new Session();
        session.setName("Once time task");
        session.addStartEventIdsItem("enrollment");
        session.setPerformanceOrder(SEQUENTIAL);

        AssessmentReference2 ref = new AssessmentReference2()
                .guid(assessment.getGuid()).appId(TEST_APP_ID)
                .title(assessmentId)
                .identifier(assessment.getIdentifier())
                .revision(assessment.getRevision().intValue());
        session.addAssessmentsItem(ref);
        session.addTimeWindowsItem(new TimeWindow().startTime("00:00").expiration("P1W"));
        schedule.addSessionsItem(session);

        schedule = schedulesApi.saveScheduleForStudy(STUDY_ID, schedule).execute().body();
        
        Timeline timeline = schedulesApi.getTimelineForStudy(STUDY_ID).execute().body();

        // Update assessment config with basic survey config.
        AssessmentConfig assessmentConfig = assessmentsApi.getAssessmentConfig(assessment.getGuid()).execute().body();
        assessmentConfig.setConfig(ASSESSMENT_CONFIG);
        assessmentsApi.updateAssessmentConfig(assessment.getGuid(), assessmentConfig).execute();

        // Get study.
        Study study = studiesApi.getStudy(STUDY_ID).execute().body();

        // Make upload. This is a zip file with two files in it, metadata.json and assessmentResult.json.
        byte[] zipFileContent = zip(SURVEY_UPLOAD_CONTENT_BY_FILENAME);

        // todo the zip file seems to be malformed.
        File tmpZipFile = File.createTempFile("test", ".zip");
        Files.write(zipFileContent, tmpZipFile);
        LOG.info("Creating temp file: " + tmpZipFile.getAbsolutePath());

        // Could also use the session instanceGuid, it doesn't matter. 
        String instanceGuid = timeline.getSchedule().get(0).getAssessments().get(0).getInstanceGuid();

        Map<String,String> userMetadata = new HashMap<>();
        userMetadata.put("instanceGuid", instanceGuid);

        Map<String,String> expectedMetadata = new HashMap<>();
        expectedMetadata.put("instanceGuid", instanceGuid);
        expectedMetadata.put("assessmentGuid", assessment.getGuid());
        expectedMetadata.put("assessmentId", assessment.getIdentifier());
        expectedMetadata.put("assessmentRevision", assessment.getRevision().toString());
        expectedMetadata.put("assessmentInstanceGuid", 
                timeline.getSchedule().get(0).getAssessments().get(0).getInstanceGuid());
        expectedMetadata.put("sessionInstanceGuid", timeline.getSchedule().get(0).getInstanceGuid());
        String sessionRefId = timeline.getSchedule().get(0).getRefGuid();
        String sessionName = timeline.getSessions().stream().filter(s -> s.getGuid().equals(sessionRefId)).findFirst().get().getLabel();
        expectedMetadata.put("sessionGuid", timeline.getSchedule().get(0).getRefGuid());
        expectedMetadata.put("sessionName", sessionName);
        expectedMetadata.put("sessionInstanceStartDay", timeline.getSchedule().get(0).getStartDay().toString());
        expectedMetadata.put("sessionInstanceEndDay", timeline.getSchedule().get(0).getEndDay().toString());
        expectedMetadata.put("sessionStartEventId", "enrollment");
        expectedMetadata.put("timeWindowGuid", timeline.getSchedule().get(0).getTimeWindowGuid());
        expectedMetadata.put("scheduleGuid", schedule.getGuid());
        expectedMetadata.put("scheduleModifiedOn", schedule.getModifiedOn().toString());
        String recordId = testUpload(zipFileContent, false, userMetadata, expectedMetadata);
        tableRowsToDelete.add(recordId);

        // Check that UploadTableRow got created
        UploadTableRow tableRow = uploadsApi.getUploadTableRowForSuperadmin(TEST_APP_ID, STUDY_ID, recordId).execute().body();
        assertNotNull(tableRow);
        assertEquals(tableRow.getAppId(), TEST_APP_ID);
        assertEquals(tableRow.getStudyId(), STUDY_ID);
        assertEquals(tableRow.getRecordId(), recordId);
        assertEquals(tableRow.getAssessmentGuid(), assessment.getGuid());
        assertNotNull(tableRow.getCreatedOn());
        assertEquals(tableRow.getHealthCode(), healthCode);
        assertEquals(tableRow.getParticipantVersion().intValue(), 1);
        assertTrue(tableRow.isTestData());

        // Table row metadata.
        Map<String, String> tableRowMetadataMap = tableRow.getMetadata();
        assertTrue(tableRowMetadataMap.get("clientInfo").contains(APP_NAME_FOR_USER));
        assertTrue(tableRowMetadataMap.get("userAgent").startsWith(APP_NAME_FOR_USER));
        assertEquals(tableRowMetadataMap.get("sessionGuid"), expectedMetadata.get("sessionGuid"));
        assertEquals(tableRowMetadataMap.get("sessionStartEventId"), expectedMetadata.get("sessionStartEventId"));
        assertEquals(tableRowMetadataMap.get("sessionName"), expectedMetadata.get("sessionName"));
        assertEquals(tableRowMetadataMap.get("dataGroups"), DATA_GROUP_SDK_INT_1 + "," +
                DATA_GROUP_TEST_USER);
        assertEquals(tableRowMetadataMap.get("externalId"), EXTERNAL_ID);
        assertEquals(tableRowMetadataMap.get("startDate"), START_DATE_STR);
        assertEquals(tableRowMetadataMap.get("endDate"), END_DATE_STR);

        // Table row data.
        Map<String, String> tableRowDataMap = tableRow.getData();
        assertEquals(tableRowDataMap.get("choiceQ1"), "1");
        assertEquals(tableRowDataMap.get("simpleQ1"), "test text");

        // Now request the actual CSV file and verify it.
        Map<String, File> csvResultFilesByName = runCsvWorker();
        assertEquals(csvResultFilesByName.size(), 1);

        // This upload is in study1, so there might be rows from old tests or other tests. Find the filename that
        // contains our assessment GUID.
        File csvFile = null;
        for (File oneCsvFile : csvResultFilesByName.values()) {
            if (oneCsvFile.getName().contains(assessment.getGuid())) {
                csvFile = oneCsvFile;
                break;
            }
        }
        assertNotNull(csvFile, "Could not find CSV file for assessment " + assessment.getGuid());

        // Since this assessment was freshly created for this test, we know there's only one row.
        Map<String, String[]> expectedRowsMap = new HashMap<>();
        expectedRowsMap.put(recordId, new String[] { recordId, STUDY_ID, study.getName(), assessment.getGuid(),
                assessmentId, "1", assessmentId, tableRow.getCreatedOn().toString(), "true", healthCode, "1",
                tableRowMetadataMap.get("clientInfo"), DATA_GROUP_SDK_INT_1 + "," + DATA_GROUP_TEST_USER, DEVICE_INFO,
                END_DATE_STR, EXTERNAL_ID, tableRowMetadataMap.get("sessionGuid"),
                tableRowMetadataMap.get("sessionName"), tableRowMetadataMap.get("sessionStartEventId"), START_DATE_STR,
                tableRowMetadataMap.get("userAgent"), "1", "test text"
        });
        UploadTableTest.assertCsvFile(csvFile, SURVEY_UPLOAD_TABLE_COLUMNS, expectedRowsMap);
    }

    @Test
    public void demographics() throws ExecutionException, IOException, InterruptedException {
        user.getClient(ForConsentedUsersApi.class)
                .changeSharingScope(new SharingScopeForm().scope(SharingScope.ALL_QUALIFIED_RESEARCHERS)).execute();

        // Get the user's healthcode.
        StudyParticipant participant = adminDeveloperWorker.getClient(ParticipantsApi.class)
                .getParticipantById(user.getUserId(), false).execute().body();
        String healthCode = participant.getHealthCode();

        // Verify version 1. There are no demographics yet, but this makes sure we wait for version 1 to finish before
        // adding version 2. Just verify that one row exists.
        // For whatever reason, the Participant Version Worker often takes up to 30 seconds to complete for version 1.
        Thread.sleep(30000);
        Future<RowSet> rowSetForApp = queryParticipantVersion(ex3Config, healthCode, "1");
        Future<RowSet> rowSetForStudy1 = queryParticipantVersion(ex3ConfigForStudy, healthCode, "1");
        Future<RowSet> rowSetForStudy2 = queryParticipantVersion(ex3ConfigForStudy2, healthCode, "1");

        assertEquals(rowSetForApp.get().getRows().size(), 1);
        assertEquals(rowSetForStudy1.get().getRows().size(), 1);
        assertEquals(rowSetForStudy2.get().getRows().size(), 1);

        // study save demographic user (version 2)
        researcher.getClient(DemographicsApi.class).saveDemographicUser(STUDY_ID, user.getUserId(),
                new DemographicUser().demographics(
                        ImmutableMap.of(
                                "category0",
                                new Demographic().multipleSelect(true).values(ImmutableList.of("value0")))))
                .execute().body();
        // study overwrite with exact same demographic user (should not create a new
        // version because last version is identical)
        researcher.getClient(DemographicsApi.class).saveDemographicUser(STUDY_ID, user.getUserId(),
                new DemographicUser().demographics(
                        ImmutableMap.of(
                                "category0",
                                new Demographic().multipleSelect(true).values(ImmutableList.of("value0")))))
                .execute().body();

        // Verify version 2.
        // Subsequent versions may take up to 25 seconds.
        Thread.sleep(25000);
        List<Map<String, String>> expectedStudy1Version2 = new ArrayList<>();
        Map<String, String> expectedStudy1Version2Row0 = makeExpectedDemographicsViewRowValueMap(STUDY_ID,
                "category0", "value0", null, null);
        expectedStudy1Version2.add(expectedStudy1Version2Row0);

        verifyDemographicsViewRowsForVersion(healthCode, 2, expectedStudy1Version2, ImmutableList.of());

        // study overwrite demographic user self (version 3)
        DemographicUserResponse overwritingDemographicUser = user
                .getClient(DemographicsApi.class).saveDemographicUserSelf(STUDY_ID,
                        new DemographicUser().demographics(
                                ImmutableMap.of(
                                        "category1",
                                        new Demographic().multipleSelect(true).values(ImmutableList.of()),
                                        "category2",
                                        new Demographic().multipleSelect(false).values(ImmutableList.of("value1"))
                                                .units("units1"))))
                .execute().body();

        // Verify version 3.
        Thread.sleep(25000);
        List<Map<String, String>> expectedStudy1Version3 = new ArrayList<>();
        Map<String, String> expectedStudy1Version3Row0 = makeExpectedDemographicsViewRowValueMap(STUDY_ID,
                "category1", null, null, null);
        expectedStudy1Version3.add(expectedStudy1Version3Row0);

        Map<String, String> expectedStudy1Version3Row1 = makeExpectedDemographicsViewRowValueMap(STUDY_ID,
                "category2", "value1", "units1", null);
        expectedStudy1Version3.add(expectedStudy1Version3Row1);

        verifyDemographicsViewRowsForVersion(healthCode, 3, expectedStudy1Version3, ImmutableList.of());

        // study delete demographic (version 4)
        researcher.getClient(DemographicsApi.class).deleteDemographic(STUDY_ID, user.getUserId(),
                overwritingDemographicUser.getDemographics().get("category1").getId()).execute();

        // Verify version 4.
        Thread.sleep(25000);
        List<Map<String, String>> expectedStudy1Version4 = new ArrayList<>();
        expectedStudy1Version4.add(expectedStudy1Version3Row1);

        verifyDemographicsViewRowsForVersion(healthCode, 4, expectedStudy1Version4, ImmutableList.of());

        // app save demographics assessment (version 5)
        adminDeveloperWorker.getClient(DemographicsApi.class)
                .saveDemographicUserAssessmentAppLevel(user.getUserId(),
                        new DemographicUserAssessment()
                                .stepHistory(ImmutableList.of(new DemographicUserAssessmentAnswerCollection()
                                        .children(ImmutableList.of(new DemographicUserAssessmentAnswer()
                                                .identifier("category3")
                                                .value(ImmutableList.of("value2", "value3")))))))
                .execute().body();

        // Verify version 5.
        Thread.sleep(25000);
        List<Map<String, String>> expectedStudy1Version5 = new ArrayList<>();
        Map<String, String> expectedStudy1Version5Row0 = makeExpectedDemographicsViewRowValueMap(null,
                "category3", "value2", null, null);
        expectedStudy1Version5.add(expectedStudy1Version5Row0);

        Map<String, String> expectedStudy1Version5Row1 = makeExpectedDemographicsViewRowValueMap(null,
                "category3", "value3", null, null);
        expectedStudy1Version5.add(expectedStudy1Version5Row1);

        expectedStudy1Version5.add(expectedStudy1Version3Row1);

        List<Map<String, String>> expectedStudy2Version5 = new ArrayList<>();
        expectedStudy2Version5.add(expectedStudy1Version5Row0);
        expectedStudy2Version5.add(expectedStudy1Version5Row1);

        verifyDemographicsViewRowsForVersion(healthCode, 5, expectedStudy1Version5, expectedStudy2Version5);

        // account update (should trigger demographics export also) (version 6)
        participant = adminDeveloperWorker.getClient(ParticipantsApi.class)
                .getParticipantById(user.getUserId(), false).execute().body();
        participant.setClientTimeZone("America/Los_Angeles");
        adminDeveloperWorker.getClient(ParticipantsApi.class).updateParticipant(user.getUserId(), participant)
                .execute();

        // Verify version 6. Has same results as version 5.
        Thread.sleep(25000);
        verifyDemographicsViewRowsForVersion(healthCode, 6, expectedStudy1Version5, expectedStudy2Version5);

        // add study demographics validation for number range and upload invalid
        // demographics (version 7)
        DemographicValuesValidationConfig numberRangeConfig = new DemographicValuesValidationConfig()
                .validationType(ValidationTypeEnum.NUMBER_RANGE)
                .validationRules(new DemographicValuesNumberRangeValidationRules().min(-20.0).max(20.0));
        studyDesigner.getClient(DemographicsApi.class)
                .saveDemographicsValidationConfig(STUDY_ID, NUMBER_VALIDATION_CATEGORY, numberRangeConfig).execute();
        DemographicUser numberValidationDemographicUser = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                NUMBER_VALIDATION_CATEGORY,
                                new Demographic().values(ImmutableList.of(2000))));
        researcher.getClient(DemographicsApi.class)
                .saveDemographicUser(STUDY_ID, user.getUserId(), numberValidationDemographicUser).execute();

        // Verify version 7.
        Thread.sleep(25000);
        List<Map<String, String>> expectedStudy1Version7 = new ArrayList<>();
        expectedStudy1Version7.add(expectedStudy1Version5Row0);
        expectedStudy1Version7.add(expectedStudy1Version5Row1);

        Map<String, String> expectedStudy1Version7Row2 = makeExpectedDemographicsViewRowValueMap(STUDY_ID,
                "numberValidationCategory", "2000", null, INVALID_NUMBER_VALUE_GREATER_THAN_MAX);
        expectedStudy1Version7.add(expectedStudy1Version7Row2);

        verifyDemographicsViewRowsForVersion(healthCode, 7, expectedStudy1Version7, expectedStudy2Version5);

        // add study demographics validation for enums and upload invalid demographics
        // (version 8)
        DemographicValuesEnumValidationRules enumRules = new DemographicValuesEnumValidationRules();
        enumRules.put("en", ImmutableList.of("foo", "bar"));
        DemographicValuesValidationConfig enumConfig = new DemographicValuesValidationConfig()
                .validationType(ValidationTypeEnum.ENUM).validationRules(enumRules);
        studyDesigner.getClient(DemographicsApi.class)
                .saveDemographicsValidationConfig(STUDY_ID, ENUM_VALIDATION_CATEGORY, enumConfig).execute();
        DemographicUser enumValidationDemographicUser = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                ENUM_VALIDATION_CATEGORY,
                                new Demographic().values(ImmutableList.of("baz"))));
        researcher.getClient(DemographicsApi.class)
                .saveDemographicUser(STUDY_ID, user.getUserId(), enumValidationDemographicUser).execute();

        // Verify version 8.
        Thread.sleep(25000);
        List<Map<String, String>> expectedStudy1Version8 = new ArrayList<>();
        expectedStudy1Version8.add(expectedStudy1Version5Row0);
        expectedStudy1Version8.add(expectedStudy1Version5Row1);

        Map<String, String> expectedStudy1Version8Row2 = makeExpectedDemographicsViewRowValueMap(STUDY_ID,
                "enumValidationCategory", "baz", null, INVALID_ENUM_VALUE);
        expectedStudy1Version8.add(expectedStudy1Version8Row2);

        verifyDemographicsViewRowsForVersion(healthCode, 8, expectedStudy1Version8, expectedStudy2Version5);

        // study delete demographic user (only app demographics should remain) (version
        // 9)
        researcher.getClient(DemographicsApi.class).deleteDemographicUser(STUDY_ID, user.getUserId()).execute();

        // Verify version 9.
        Thread.sleep(25000);
        verifyDemographicsViewRowsForVersion(healthCode, 9, expectedStudy2Version5, expectedStudy2Version5);

        // add app demographics validation for number range and upload invalid
        // demographics (version 10)
        adminDeveloperWorker.getClient(DemographicsApi.class)
                .saveDemographicsValidationConfigAppLevel(NUMBER_VALIDATION_CATEGORY, numberRangeConfig).execute();
        adminDeveloperWorker.getClient(DemographicsApi.class)
                .saveDemographicUserAppLevel(user.getUserId(), numberValidationDemographicUser)
                .execute();

        // Verify version 10.
        Thread.sleep(25000);
        List<Map<String, String>> expectedVersion10 = new ArrayList<>();
        Map<String, String> expectedVersion10Row0 = makeExpectedDemographicsViewRowValueMap(null,
                "numberValidationCategory", "2000", null, INVALID_NUMBER_VALUE_GREATER_THAN_MAX);
        expectedVersion10.add(expectedVersion10Row0);

        verifyDemographicsViewRowsForVersion(healthCode, 10, expectedVersion10, expectedVersion10);

        // add app demographics validation for enums and upload invalid demographics
        // (version 11)
        adminDeveloperWorker.getClient(DemographicsApi.class)
                .saveDemographicsValidationConfigAppLevel(ENUM_VALIDATION_CATEGORY, enumConfig).execute();
        adminDeveloperWorker.getClient(DemographicsApi.class)
                .saveDemographicUserAppLevel(user.getUserId(), enumValidationDemographicUser)
                .execute();

        // Verify version 11.
        Thread.sleep(25000);
        List<Map<String, String>> expectedVersion11 = new ArrayList<>();
        Map<String, String> expectedVersion11Row0 = makeExpectedDemographicsViewRowValueMap(null,
                "enumValidationCategory", "baz", null, INVALID_ENUM_VALUE);
        expectedVersion11.add(expectedVersion11Row0);

        List<SelectColumn> demographicsViewHeaderList = verifyDemographicsViewRowsForVersion(healthCode,
                11, expectedVersion11, expectedVersion11);

        // check demographics table column names
        // Make a dummy query with 1 row. We only want the headers anyway.
        String demographicsTableQuery = "SELECT * FROM " + ex3Config.getParticipantVersionDemographicsTableId() +
                " WHERE healthCode = '" + healthCode + "' AND participantVersion=11";
        Future<RowSet> demographicsTableRowSet = querySynapseTable(ex3Config.getParticipantVersionDemographicsTableId(),
                demographicsTableQuery);
        verifySynapseTableHeaders(demographicsTableRowSet.get().getHeaders(),
                ImmutableList.of("healthCode", "participantVersion", "studyId", "demographicCategoryName",
                        "demographicValue", "demographicUnits", "demographicInvalidity"));

        // check study demographics view column names
        verifySynapseTableHeaders(demographicsViewHeaderList,
                ImmutableList.of("healthCode", "participantVersion", "createdOn", "modifiedOn", "dataGroups",
                        "languages", "sharingScope", "studyMemberships", "clientTimeZone", "studyId",
                        "demographicCategoryName", "demographicValue", "demographicUnits", "demographicInvalidity"));

    }

    private Map<String, String> makeExpectedDemographicsViewRowValueMap(String studyId, String categoryName,
            String value, String units, String invalidity) {
        Map<String, String> rowValueMap = new HashMap<>();
        rowValueMap.put("studyId", studyId);
        rowValueMap.put("demographicCategoryName", categoryName);
        rowValueMap.put("demographicValue", value);
        rowValueMap.put("demographicUnits", units);
        rowValueMap.put("demographicInvalidity", invalidity);
        return rowValueMap;
    }

    private static String makeDemographicsViewQuery(String tableId, String healthCode, int versionNum) {
        return "SELECT * FROM " + tableId + " WHERE healthCode='" + healthCode + "' AND participantVersion=" +
                versionNum + " order by studyId, demographicCategoryName, demographicValue";
    }

    private static Map<String, String> convertRowToMap(List<String> headerNameList, Row row) {
        List<String> valueList = row.getValues();
        assertEquals(headerNameList.size(), valueList.size());
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < headerNameList.size(); i++) {
            map.put(headerNameList.get(i), valueList.get(i));
        }
        return map;
    }

    // Actual rows and expected rows should be in the same order. Query is ordered by studyId,
    // then demographicCategoryName, then demographicValue.
    // Returns the headers.
    private List<SelectColumn> verifyDemographicsViewRowsForVersion(String healthCode, int versionNum,
            List<Map<String, String>> expectedRowsForStudy1, List<Map<String, String>> expectedRowsForStudy2)
            throws ExecutionException, InterruptedException {
        // Query Synapse tables.
        String queryForApp = makeDemographicsViewQuery(ex3Config.getParticipantVersionDemographicsViewId(),
                healthCode, versionNum);
        String queryForStudy1 = makeDemographicsViewQuery(ex3ConfigForStudy.getParticipantVersionDemographicsViewId(),
                healthCode, versionNum);
        String queryForStudy2 = makeDemographicsViewQuery(ex3ConfigForStudy2.getParticipantVersionDemographicsViewId(),
                healthCode, versionNum);

        Future<RowSet> futureForApp = querySynapseTable(ex3Config.getParticipantVersionDemographicsViewId(),
                queryForApp);
        Future<RowSet> futureForStudy1 = querySynapseTable(ex3ConfigForStudy.getParticipantVersionDemographicsViewId(),
                queryForStudy1);
        Future<RowSet> futureForStudy2 = querySynapseTable(ex3ConfigForStudy2.getParticipantVersionDemographicsViewId(),
                queryForStudy2);

        // Get headers.
        RowSet rowSetForApp = futureForApp.get();
        List<SelectColumn> headersForApp = rowSetForApp.getHeaders();
        List<String> headerNameList = headersForApp.stream().map(SelectColumn::getName).collect(Collectors.toList());

        // Verify rows. Note that app and study1 have the same expected rows.
        verifyDemographicsViewRowSet(rowSetForApp, headerNameList, expectedRowsForStudy1);
        verifyDemographicsViewRowSet(futureForStudy1.get(), headerNameList, expectedRowsForStudy1);
        verifyDemographicsViewRowSet(futureForStudy2.get(), headerNameList, expectedRowsForStudy2);

        return headersForApp;
    }

    private static void verifyDemographicsViewRowSet(RowSet rowSet, List<String> expectedHeaders,
            List<Map<String, String>> expectedRows) {
        List<Row> rowList = rowSet.getRows();

        // Verify we have the same headers.
        List<SelectColumn> headerList = rowSet.getHeaders();
        for (int i = 0; i < headerList.size(); i++) {
            assertEquals(headerList.get(i).getName(), expectedHeaders.get(i));
        }

        // Verify rows.
        assertEquals(rowList.size(), expectedRows.size());
        if (!expectedRows.isEmpty()) {
            // Verify each row.
            for (int i = 0; i < rowList.size(); i++) {
                Map<String, String> rowValueMap = convertRowToMap(expectedHeaders, rowList.get(i));
                for (Map.Entry<String, String> expectedValuePair : expectedRows.get(i).entrySet()) {
                    assertTrue(rowValueMap.containsKey(expectedValuePair.getKey()));
                    assertEquals(rowValueMap.get(expectedValuePair.getKey()), expectedValuePair.getValue());
                }
            }
        }
    }

    private void verifySynapseTableHeaders(List<SelectColumn> headerList, List<String> expectedColumnNames) {
        assertEquals(headerList.size(), expectedColumnNames.size());
        for (int i = 0; i < expectedColumnNames.size(); i++) {
            assertEquals(headerList.get(i).getName(), expectedColumnNames.get(i));
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static byte[] zip(Map<String, String> contentByFileName) throws IOException {
        // The double nested try loop is required, because we need to close the ZipOutputStream before we convert the
        // ByteArrayOutputStream into bytes.
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            try (ZipOutputStream zipOutputStream = new ZipOutputStream(byteArrayOutputStream)) {
                for (Map.Entry<String, String> contentEntry : contentByFileName.entrySet()) {
                    String contentFilename = contentEntry.getKey();
                    String content = contentEntry.getValue();

                    ZipEntry oneZipEntry = new ZipEntry(contentFilename);
                    zipOutputStream.putNextEntry(oneZipEntry);
                    zipOutputStream.write(content.getBytes(StandardCharsets.UTF_8));
                    zipOutputStream.closeEntry();
                }
            }

            return byteArrayOutputStream.toByteArray();
        }
    }

    private void testUpload(byte[] content, boolean encrypted) throws Exception {
        testUpload(content, encrypted,
                ImmutableMap.of(CUSTOM_METADATA_KEY, CUSTOM_METADATA_VALUE),
                ImmutableMap.of(CUSTOM_METADATA_KEY_SANITIZED, CUSTOM_METADATA_VALUE));
    }
    
    private String testUpload(byte[] content, boolean encrypted, Map<String,String> userMetadata, Map<String,String> expectedMetadata) throws Exception {
        // Participants created by TestUserHelper (UserAdminService) are set to no_sharing by default. Enable sharing
        // so that the test can succeed.
        ParticipantsApi participantsApi = user.getClient(ParticipantsApi.class);
        StudyParticipant participant = participantsApi.getUsersParticipantRecord(false).execute().body();
        participant.setSharingScope(SharingScope.ALL_QUALIFIED_RESEARCHERS);
        participantsApi.updateUsersParticipantRecord(participant).execute();

        // Subscribe to export notifications for app and study.
        ForAdminsApi adminsApi = adminDeveloperWorker.getClient(ForAdminsApi.class);
        ExporterSubscriptionRequest exporterSubscriptionRequest = new ExporterSubscriptionRequest();
        exporterSubscriptionRequest.putAttributesItem("RawMessageDelivery", "true");
        exporterSubscriptionRequest.setEndpoint(testQueueArn);
        exporterSubscriptionRequest.setProtocol("sqs");
        ExporterSubscriptionResult exporterSubscriptionResult = adminsApi.subscribeToExportNotificationsForApp(
                exporterSubscriptionRequest).execute().body();
        subscriptionArnList.add(exporterSubscriptionResult.getSubscriptionArn());

        exporterSubscriptionResult = adminsApi.subscribeToExportNotificationsForStudy(STUDY_ID,
                exporterSubscriptionRequest).execute().body();
        subscriptionArnList.add(exporterSubscriptionResult.getSubscriptionArn());

        // Upload file to Bridge.
        UploadInfo uploadInfo = uploadFile(content, encrypted, userMetadata, expectedMetadata);
        String filename = uploadInfo.filename;
        String uploadId = uploadInfo.uploadId;

        // Verify Synapse and S3.
        ExportedRecordInfo appRecordInfo = verifyUpload(ex3Config, uploadId, filename,
                false, expectedMetadata, content);
        ExportedRecordInfo studyRecordInfo = verifyUpload(ex3ConfigForStudy, uploadId, filename,
                true, expectedMetadata, content);

        // Verify the record in Bridge.
        ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);
        HealthDataRecordEx3 record = usersApi.getRecordEx3ById(uploadId, "true").execute().body();
        assertTrue(record.isExported());
        DateTime oneHourAgo = DateTime.now().minusHours(1);
        assertTrue(record.getExportedOn().isAfter(oneHourAgo));
        assertRecordInfoEquals(appRecordInfo, record.getExportedRecord());
        assertRecordInfoEquals(studyRecordInfo, record.getExportedStudyRecords().get(STUDY_ID));

        // Verify the presigned download url for a health record is generated and contains the data expected.
        String url = record.getDownloadUrl();
        HttpResponse responseForPresignedUrl = Request.Get(url).execute().returnResponse();
        byte[] contentForPresignedUrl = EntityUtils.toByteArray(responseForPresignedUrl.getEntity());
        assertEquals(200, responseForPresignedUrl.getStatusLine().getStatusCode());
        assertEquals(content, contentForPresignedUrl);

        DateTime fiftyMinsAfter = DateTime.now().plusMinutes(50);
        assertTrue(record.getDownloadExpiration().isAfter(fiftyMinsAfter));

        // Receive notifications. Because we use a Standard queue and not an SQS queue, the messages can arrive in any
        // order.
        boolean foundAppNotification = false;
        boolean foundStudyNotification = false;

        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
        receiveMessageRequest.setQueueUrl(testQueueUrl);
        receiveMessageRequest.setWaitTimeSeconds(10);
        receiveMessageRequest.setMaxNumberOfMessages(2);

        // Even with setMaxNumberOfMessages(2), SQS may return only 1 result. Call this in a loop until we have 2 total
        // messages.
        List<Message> allResultsList = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            List<Message> resultList = sqsClient.receiveMessage(receiveMessageRequest).getMessages();
            allResultsList.addAll(resultList);
            if (allResultsList.size() >= 2) {
                break;
            }
        }
        assertTrue(allResultsList.size() >= 2);
        for (Message exportNotificationResult : allResultsList) {
            String exportNotificationJsonText = exportNotificationResult.getBody();
            JsonNode exportNotificationNode = DefaultObjectMapper.INSTANCE.readTree(exportNotificationJsonText);
            if ("ExportToAppNotification".equals(exportNotificationNode.get("type").textValue())) {
                foundAppNotification = true;
                assertEquals(exportNotificationNode.get("appId").textValue(), TEST_APP_ID);
                assertEquals(exportNotificationNode.get("recordId").textValue(), uploadId);

                assertEquals(exportNotificationNode.get("record").get("parentProjectId").textValue(),
                        appRecordInfo.getParentProjectId());
                assertEquals(exportNotificationNode.get("record").get("rawFolderId").textValue(),
                        appRecordInfo.getRawFolderId());
                assertEquals(exportNotificationNode.get("record").get("fileEntityId").textValue(),
                        appRecordInfo.getFileEntityId());
                assertEquals(exportNotificationNode.get("record").get("s3Bucket").textValue(),
                        appRecordInfo.getS3Bucket());
                assertEquals(exportNotificationNode.get("record").get("s3Key").textValue(),
                        appRecordInfo.getS3Key());

                assertEquals(exportNotificationNode.get("studyRecords").get(STUDY_ID).get("parentProjectId").textValue(),
                        studyRecordInfo.getParentProjectId());
                assertEquals(exportNotificationNode.get("studyRecords").get(STUDY_ID).get("rawFolderId").textValue(),
                        studyRecordInfo.getRawFolderId());
                assertEquals(exportNotificationNode.get("studyRecords").get(STUDY_ID).get("fileEntityId").textValue(),
                        studyRecordInfo.getFileEntityId());
                assertEquals(exportNotificationNode.get("studyRecords").get(STUDY_ID).get("s3Bucket").textValue(),
                        studyRecordInfo.getS3Bucket());
                assertEquals(exportNotificationNode.get("studyRecords").get(STUDY_ID).get("s3Key").textValue(),
                        studyRecordInfo.getS3Key());

                // Don't check study2. Most studies export to both studies, but a few only export to study1.
            } else if ("ExportToStudyNotification".equals(exportNotificationNode.get("type").textValue())) {
                foundStudyNotification = true;
                assertEquals(exportNotificationNode.get("appId").textValue(), TEST_APP_ID);
                assertEquals(exportNotificationNode.get("studyId").textValue(), STUDY_ID);
                assertEquals(exportNotificationNode.get("recordId").textValue(), uploadId);

                assertEquals(exportNotificationNode.get("parentProjectId").textValue(),
                        studyRecordInfo.getParentProjectId());
                assertEquals(exportNotificationNode.get("rawFolderId").textValue(),
                        studyRecordInfo.getRawFolderId());
                assertEquals(exportNotificationNode.get("fileEntityId").textValue(),
                        studyRecordInfo.getFileEntityId());
                assertEquals(exportNotificationNode.get("s3Bucket").textValue(),
                        studyRecordInfo.getS3Bucket());
                assertEquals(exportNotificationNode.get("s3Key").textValue(),
                        studyRecordInfo.getS3Key());
            }
        }

        assertTrue(foundAppNotification, "Found app notification");
        assertTrue(foundStudyNotification, "Found study notification");
        return record.getId();
    }

    // This exists because type is not settable, so we can't just call .equals().
    private void assertRecordInfoEquals(ExportedRecordInfo record1, ExportedRecordInfo record2) {
        assertEquals(record1.getFileEntityId(), record2.getFileEntityId());
        assertEquals(record1.getParentProjectId(), record2.getParentProjectId());
        assertEquals(record1.getRawFolderId(), record2.getRawFolderId());
        assertEquals(record1.getS3Bucket(), record2.getS3Bucket());
        assertEquals(record1.getS3Key(), record2.getS3Key());
    }

    private ExportedRecordInfo verifyUpload(Exporter3Configuration ex3Config, String uploadId, String filename, boolean isForStudy,
            Map<String,String> expectedMetadata, byte[] expectedContent) throws Exception {
        // Verify Synapse file entity and annotations.
        String rawFolderId = ex3Config.getRawDataFolderId();
        String todaysDateString = LocalDate.now(TestUtils.LOCAL_TIME_ZONE).toString();
        String exportedFilename = uploadId + '-' + filename;

        // First, get the exported file.
        String todayFolderId = getSynapseChildByName(rawFolderId, todaysDateString);
        String exportedFileId = getSynapseChildByName(todayFolderId, exportedFilename);

        // Now verify the annotations.
        Annotations annotations = synapseHelper.getAnnotationsWithRetry(exportedFileId);
        Map<String, AnnotationsValue> annotationMap = annotations.getAnnotations();
        Map<String, String> flattenedAnnotationMap = new HashMap<>();
        for (Map.Entry<String, AnnotationsValue> annotationEntry : annotationMap.entrySet()) {
            String annotationKey = annotationEntry.getKey();
            AnnotationsValue annotationsValue = annotationEntry.getValue();
            if ("participantVersion".equals(annotationKey)) {
                // participantVersion is special. This needs to be joined with the ParticipantVersion table, so it's
                // a number.
                assertEquals(annotationsValue.getType(), AnnotationsValueType.LONG);
            } else {
                assertEquals(annotationsValue.getType(), AnnotationsValueType.STRING);
            }
            assertEquals(annotationsValue.getValue().size(), 1);
            flattenedAnnotationMap.put(annotationKey, annotationsValue.getValue().get(0));
        }
        verifyMetadata(flattenedAnnotationMap, uploadId, expectedMetadata);

        // Get the STS token and verify the file in S3.
        AWSCredentialsProvider awsCredentialsProvider = new SynapseStsCredentialsProvider(synapseClient, rawFolderId,
                StsPermission.read_only);
        AmazonS3Client s3Client = new AmazonS3Client(awsCredentialsProvider).withRegion(Regions.US_EAST_1);
        S3Helper s3Helper = new S3Helper();
        s3Helper.setS3Client(s3Client);

        String expectedS3Key;
        if (isForStudy) {
            expectedS3Key = IntegTestUtils.TEST_APP_ID + '/' + STUDY_ID + '/' + todaysDateString + '/' +
                    exportedFilename;
        } else {
            expectedS3Key = IntegTestUtils.TEST_APP_ID + '/' + todaysDateString + '/' + exportedFilename;
        }
        byte[] s3Bytes = s3Helper.readS3FileAsBytes(rawDataBucket, expectedS3Key);
        assertEquals(s3Bytes, expectedContent);

        ObjectMetadata s3Metadata = s3Helper.getObjectMetadata(rawDataBucket, expectedS3Key);
        assertEquals(s3Metadata.getSSEAlgorithm(), ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        assertEquals(s3Metadata.getContentType(), CONTENT_TYPE_TEXT_PLAIN);
        verifyMetadata(s3Metadata.getUserMetadata(), uploadId, expectedMetadata);

        // Verify the Participant Version table.
        String healthCode = flattenedAnnotationMap.get("healthCode");
        String participantVersionStr = flattenedAnnotationMap.get("participantVersion");
        RowSet queryRowSet = queryParticipantVersion(ex3Config, healthCode, participantVersionStr).get();
        List<SelectColumn> columnList = queryRowSet.getHeaders();
        assertEquals(queryRowSet.getRows().size(), 1);
        List<String> rowValueList = queryRowSet.getRows().get(0).getValues();
        assertEquals(columnList.size(), rowValueList.size());
        Map<String, String> rowMap = new HashMap<>();
        for (int i = 0; i < columnList.size(); i++) {
            rowMap.put(columnList.get(i).getName(), rowValueList.get(i));
        }

        assertEquals(rowMap.get("healthCode"), healthCode);
        assertEquals(rowMap.get("participantVersion"), participantVersionStr);
        assertEquals(rowMap.get("sharingScope"), "all_qualified_researchers");

        // Timestamps are relatively recent. Because of clock skew on Jenkins, give a very generous time window of,
        // let's say, 1 hour.
        DateTime oneHourAgo = DateTime.now().minusHours(1);

        long participantCreatedOn = Long.parseLong(rowMap.get("createdOn"));
        assertTrue(participantCreatedOn > oneHourAgo.getMillis());

        long participantModifiedOn = Long.parseLong(rowMap.get("modifiedOn"));
        assertTrue(participantModifiedOn > oneHourAgo.getMillis());

        // Return the record info, so that we can verify notifications later on in our test.
        ExportedRecordInfo recordInfo = new ExportedRecordInfo();
        recordInfo.setParentProjectId(ex3Config.getProjectId());
        recordInfo.setRawFolderId(todayFolderId);
        recordInfo.setFileEntityId(exportedFileId);
        recordInfo.setS3Bucket(rawDataBucket);
        recordInfo.setS3Key(expectedS3Key);
        return recordInfo;
    }

    private Future<RowSet> queryParticipantVersion(Exporter3Configuration ex3Config, String healthCode,
            String participantVersionStr) {
        // Query participant version table.
        String participantVersionTableId = ex3Config.getParticipantVersionTableId();
        String query = "SELECT * FROM " + participantVersionTableId + " WHERE healthCode='" + healthCode + "'";
        if (participantVersionStr != null) {
            query += " and participantVersion=" + participantVersionStr;
        }
        return querySynapseTable(participantVersionTableId, query);
    }

    private Future<RowSet> querySynapseTable(String tableId, String query) {
        return EXECUTOR_SERVICE.submit(() -> synapseHelper.queryTableEntityBundle(query, tableId).getQueryResult()
                .getQueryResults());
    }

    private static class UploadInfo {
        String filename;
        String uploadId;
    }
    
    private UploadInfo uploadFile(byte[] content, boolean encrypted) throws InterruptedException, IOException {
        return uploadFile(content, encrypted, ImmutableMap.of(CUSTOM_METADATA_KEY, CUSTOM_METADATA_VALUE),
                ImmutableMap.of(CUSTOM_METADATA_KEY_SANITIZED, CUSTOM_METADATA_VALUE));        
    }

    private UploadInfo uploadFile(byte[] content, boolean encrypted, Map<String, String> userMetadata,
            Map<String, String> expectedMetadata) throws InterruptedException, IOException {
        // Create a temp file so that we can use RestUtils.
        File file = File.createTempFile("text", ".txt");
        String filename = file.getName();
        Files.write(content, file);

        // Create upload request. We want to add custom metadata. Also, RestUtils defaults to application/zip. We want
        // to overwrite this.
        UploadRequest uploadRequest = RestUtils.makeUploadRequestForFile(file);
        uploadRequest.setContentType(CONTENT_TYPE_TEXT_PLAIN);
        for (Map.Entry<String,String> entry : userMetadata.entrySet()) {
            uploadRequest.putMetadataItem(entry.getKey(), entry.getValue());    
        }
        uploadRequest.setEncrypted(encrypted);
        uploadRequest.setZipped(false);

        // Upload.
        ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);
        UploadSession session = usersApi.requestUploadSession(uploadRequest).execute().body();
        String uploadId = session.getId();
        RestUtils.uploadToS3(file, session.getUrl(), CONTENT_TYPE_TEXT_PLAIN);
        usersApi.completeUploadSession(session.getId(), true, false).execute();

        // Sleep for a bit to give the Worker Exporter time to finish.
        Thread.sleep(2000);

        UploadInfo uploadInfo = new UploadInfo();
        uploadInfo.filename = filename;
        uploadInfo.uploadId = uploadId;
        return uploadInfo;
    }

    private String getSynapseChildByName(String parentId, String childName) throws SynapseException {
        try {
            return synapseHelper.lookupChildWithRetry(parentId, childName);
        } catch (SynapseNotFoundException ex) {
            return null;
        }
    }

    private static void verifyMetadata(Map<String, String> metadataMap, String expectedRecordId, Map<String,String> expectedValues)
    throws Exception {
        assertEquals(metadataMap.size(), 8 + expectedValues.size());
        assertTrue(metadataMap.containsKey("healthCode"));
        assertTrue(metadataMap.containsKey("contentType"));
        assertEquals(metadataMap.get("contentType"), CONTENT_TYPE_TEXT_PLAIN);
        assertEquals(metadataMap.get("participantVersion"), "1");
        assertEquals(metadataMap.get("recordId"), expectedRecordId);
        for (String key : expectedValues.keySet()) {
            assertEquals(metadataMap.get(key), expectedValues.get(key), key + " has invalid value");
        }

        // Client info exists and is in JSON format. Verify the correct app name from client info.
        String clientInfoJsonText = metadataMap.get("clientInfo");
        JsonNode jsonNode = DefaultObjectMapper.INSTANCE.readTree(clientInfoJsonText);
        assertTrue(jsonNode.isObject());
        assertEquals(jsonNode.get("appName").textValue(), APP_NAME_FOR_USER);

        // User-Agent exists. Because the client can add additional default fields, just check that it starts with
        // APP_NAME_FOR_USER.
        assertTrue(metadataMap.get("userAgent").startsWith(APP_NAME_FOR_USER));

        // Timestamps are relatively recent. Because of clock skew on Jenkins, give a very generous time window of,
        // let's say, 1 hour.
        DateTime oneHourAgo = DateTime.now().minusHours(1);
        DateTime exportedOn = DateTime.parse(metadataMap.get("exportedOn"));
        assertTrue(exportedOn.isAfter(oneHourAgo));

        DateTime uploadedOn = DateTime.parse(metadataMap.get("uploadedOn"));
        assertTrue(uploadedOn.isAfter(oneHourAgo));
    }

    private static Map<String, File> runCsvWorker() throws Exception {
        // We need to know the previous finish time so we can determine when the worker is finished.
        long previousFinishTime = TestUtils.getWorkerLastFinishedTime(ddbWorkerLogTable,
                UploadTableTest.WORKER_ID_CSV_WORKER);

        // Create request.
        UploadsApi researcherUploadsApi = researcher.getClient(UploadsApi.class);
        String jobGuid = researcherUploadsApi.requestUploadTableForStudy(STUDY_ID).execute().body().getJobGuid();

        // Wait until the worker is finished.
        // Note that the server has logic to dedupe CSV requests every 5 minutes. If you run this test more than once
        // per 5 minutes, it will fail. If you need to do this for whatever reason, you should temporarily comment out
        // that code in BridgeServer. (This should never come up under normal situations.)
        TestUtils.pollWorkerLog(ddbWorkerLogTable, UploadTableTest.WORKER_ID_CSV_WORKER, previousFinishTime);

        // Get the S3 URL from Bridge Server.
        UploadTableJobResult jobResult = researcherUploadsApi.getUploadTableJobResult(STUDY_ID, jobGuid).execute()
                .body();
        assertEquals(jobResult.getStatus(), UploadTableJobStatus.SUCCEEDED);
        assertNotNull(jobResult.getUrl());
        assertNotNull(jobResult.getExpiresOn());

        HttpResponse responseForPresignedUrl = Request.Get(jobResult.getUrl()).execute().returnResponse();
        assertEquals(200, responseForPresignedUrl.getStatusLine().getStatusCode());

        Map<String, File> filesByName;
        try (InputStream inputStream = responseForPresignedUrl.getEntity().getContent()) {
            filesByName = UploadTableTest.unzipStream(inputStream);
        }

        return filesByName;
    }
}
