package org.sagebionetworks.bridge.exporter.integration;

import static org.sagebionetworks.bridge.rest.model.PerformanceOrder.SEQUENTIAL;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_ID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
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
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.annotation.v2.Annotations;
import org.sagebionetworks.repo.model.annotation.v2.AnnotationsValue;
import org.sagebionetworks.repo.model.annotation.v2.AnnotationsValueType;
import org.sagebionetworks.repo.model.sts.StsPermission;
import org.sagebionetworks.repo.model.table.QueryResult;
import org.sagebionetworks.repo.model.table.QueryResultBundle;
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
import org.sagebionetworks.bridge.rest.api.AssessmentsApi;
import org.sagebionetworks.bridge.rest.api.DemographicsApi;
import org.sagebionetworks.bridge.rest.api.ForAdminsApi;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForDevelopersApi;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.api.ParticipantsApi;
import org.sagebionetworks.bridge.rest.api.SchedulesV2Api;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.Assessment;
import org.sagebionetworks.bridge.rest.model.AssessmentReference2;
import org.sagebionetworks.bridge.rest.model.ClientInfo;
import org.sagebionetworks.bridge.rest.model.Demographic;
import org.sagebionetworks.bridge.rest.model.DemographicUser;
import org.sagebionetworks.bridge.rest.model.DemographicUserAssessment;
import org.sagebionetworks.bridge.rest.model.DemographicUserAssessmentAnswer;
import org.sagebionetworks.bridge.rest.model.DemographicUserAssessmentAnswerCollection;
import org.sagebionetworks.bridge.rest.model.DemographicUserResponse;
import org.sagebionetworks.bridge.rest.model.Enrollment;
import org.sagebionetworks.bridge.rest.model.Exporter3Configuration;
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
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.SqsHelper;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;
import org.sagebionetworks.bridge.util.IntegTestUtils;

@SuppressWarnings({ "ConstantConditions", "deprecation" })
public class Exporter3Test {
    private static final Logger LOG = LoggerFactory.getLogger(Exporter3Test.class);

    private static final String CONFIG_KEY_BACKFILL_BUCKET = "backfill.bucket";
    private static final String CONFIG_KEY_RAW_DATA_BUCKET = "health.data.bucket.raw";
    private static final String CONTENT_TYPE_TEXT_PLAIN = "text/plain";
    private static final String CUSTOM_METADATA_KEY = "custom-metadata-key";
    private static final String CUSTOM_METADATA_KEY_SANITIZED = "custom_metadata_key";
    private static final String CUSTOM_METADATA_VALUE = "custom-metadata-value";
    private static final String STUDY_ID = "study1";
    private static final String STUDY_2_ID = "study2";
    private static final byte[] UPLOAD_CONTENT = "This is the upload content".getBytes(StandardCharsets.UTF_8);
    private static final String WORKER_ID_BACKFILL_PARTICIPANTS = "BackfillParticipantVersionsWorker";
    private static final String WORKER_ID_REDRIVE_PARTICIPANTS = "RedriveParticipantVersionsWorker";
    private static final String PARTICIPANT_VERSIONS_TABLE_ORDER_BY = "participantVersion";
    private static final String PARTICIPANT_VERSIONS_DEMOGRAPHICS_VIEW_ORDER_BY = "participantVersion, appId, studyId, demographicCategoryName, demographicValue, demographicUnits";

    private static final String APP_NAME_FOR_USER = "app-name-for-user";
    private static final ClientInfo CLIENT_INFO_FOR_USER = new ClientInfo().appName(APP_NAME_FOR_USER);

    private static TestUser adminDeveloperWorker;
    private static String backfillBucket;
    private static Table ddbWorkerLogTable;
    private static Exporter3Configuration ex3Config;
    private static Exporter3Configuration ex3ConfigForStudy;
    private static Exporter3Configuration ex3ConfigForStudy2;
    private static String rawDataBucket;
    private static S3Helper s3Helper;
    private static SqsHelper sqsHelper;
    private static SynapseClient synapseClient;
    private static String workerSqsUrl;

    private TestUser user;
    private Schedule2 schedule;
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

        workerSqsUrl = config.get("worker.request.sqs.queue.url");
        sqsHelper = TestUtils.getSqsHelper(awsCredentialsProvider);

        // Set up SynapseClient.
        synapseClient = TestUtils.getSynapseClient(config);

        // Create admin account.
        adminDeveloperWorker = TestUserHelper.createAndSignInUser(Exporter3Test.class, false, Role.ADMIN,
                Role.DEVELOPER, Role.WORKER);

        // Wipe the Exporter 3 Config and re-create it.
        deleteEx3Resources();

        // Init Exporter 3.
        ForAdminsApi adminsApi = adminDeveloperWorker.getClient(ForAdminsApi.class);
        ex3Config = adminsApi.initExporter3().execute().body();
        ex3ConfigForStudy = adminsApi.initExporter3ForStudy(STUDY_ID).execute().body();
        ex3ConfigForStudy2 = adminsApi.initExporter3ForStudy(STUDY_2_ID).execute().body();
    }

    @BeforeMethod
    public void before() throws Exception {
        // Note: Consent also enrolls the participant in study1.
        user = new TestUserHelper.Builder(Exporter3Test.class).withClientInfo(CLIENT_INFO_FOR_USER)
                .withConsentUser(true).createAndSignInUser();
        adminDeveloperWorker.getClient(StudiesApi.class)
                .enrollParticipant(STUDY_2_ID, new Enrollment().userId(user.getUserId())).execute().body();
    }

    @AfterMethod
    public void after() throws Exception {
        if (user != null) {
            user.signOutAndDeleteUser();
        }
        TestUser admin = TestUserHelper.getSignedInAdmin();
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

        if (adminDeveloperWorker != null) {
            adminDeveloperWorker.signOutAndDeleteUser();
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

        //There is now 1 participant version.
        participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID, userId).execute().body()
                .getItems();
        assertEquals(1, participantVersionList.size());

        // Participant version was exported to app's Synapse project.
        RowSet particpantVersionRowSet = queryParticipantVersion(ex3Config, healthCode, null);
        assertEquals(particpantVersionRowSet.getRows().size(), 1);

        // And for study's Synapse project.
        particpantVersionRowSet = queryParticipantVersion(ex3ConfigForStudy, healthCode, null);
        assertEquals(particpantVersionRowSet.getRows().size(), 1);
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
        RowSet particpantVersionRowSet = queryParticipantVersion(ex3Config, healthCode, null);
        assertEquals(particpantVersionRowSet.getRows().size(), 1);

        // And for study's Synapse project.
        particpantVersionRowSet = queryParticipantVersion(ex3ConfigForStudy, healthCode, null);
        assertEquals(particpantVersionRowSet.getRows().size(), 1);

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
        particpantVersionRowSet = queryParticipantVersion(ex3Config, healthCode, null);
        assertEquals(particpantVersionRowSet.getRows().size(), 2);

        // And for study's Synapse project.
        particpantVersionRowSet = queryParticipantVersion(ex3ConfigForStudy, healthCode, null);
        assertEquals(particpantVersionRowSet.getRows().size(), 2);
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
        SchedulesV2Api schedulesApi = admin.getClient(SchedulesV2Api.class);
        
        String assessmentId = getClass().getSimpleName() + "-" + RandomStringUtils.randomAlphabetic(10);
        
        assessment = new Assessment().title(assessmentId).osName("Universal").ownerId("sage-bionetworks")
                .identifier(assessmentId);
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
        expectedMetadata.put("sessionGuid", timeline.getSchedule().get(0).getRefGuid());
        expectedMetadata.put("sessionInstanceStartDay", timeline.getSchedule().get(0).getStartDay().toString());
        expectedMetadata.put("sessionInstanceEndDay", timeline.getSchedule().get(0).getEndDay().toString());
        expectedMetadata.put("sessionStartEventId", "enrollment");
        expectedMetadata.put("timeWindowGuid", timeline.getSchedule().get(0).getTimeWindowGuid());
        expectedMetadata.put("scheduleGuid", schedule.getGuid());
        expectedMetadata.put("scheduleModifiedOn", schedule.getModifiedOn().toString());
        testUpload(UPLOAD_CONTENT, false, userMetadata, expectedMetadata);
    }

    @Test
    public void demographics() throws IOException, SynapseException, InterruptedException, TimeoutException {
        user.getClient(ForConsentedUsersApi.class)
                .changeSharingScope(new SharingScopeForm().scope(SharingScope.ALL_QUALIFIED_RESEARCHERS)).execute();
        TestUser researcher = TestUserHelper.createAndSignInUser(Exporter3Test.class, false, Role.RESEARCHER);

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
        // study delete demographic (version 4)
        researcher.getClient(DemographicsApi.class).deleteDemographic(STUDY_ID, user.getUserId(),
                overwritingDemographicUser.getDemographics().get("category1").getId()).execute();
        // app save demographics assessment (version 5)
        adminDeveloperWorker.getClient(DemographicsApi.class)
                .saveDemographicUserAssessmentAppLevel(user.getUserId(),
                        new DemographicUserAssessment()
                                .stepHistory(ImmutableList.of(new DemographicUserAssessmentAnswerCollection()
                                        .children(ImmutableList.of(new DemographicUserAssessmentAnswer()
                                                .identifier("category3")
                                                .value(ImmutableList.of("value2", "value3")))))))
                .execute().body();
        // account update (should trigger demographics export also) (version 6)
        StudyParticipant participant = adminDeveloperWorker.getClient(ParticipantsApi.class)
                .getParticipantById(user.getUserId(), false).execute().body();
        participant.setClientTimeZone("America/Los_Angeles");
        adminDeveloperWorker.getClient(ParticipantsApi.class).updateParticipant(user.getUserId(), participant)
                .execute();
        // study delete demographic user (only app demographics should remain) (version
        // 7)
        researcher.getClient(DemographicsApi.class).deleteDemographicUser(STUDY_ID, user.getUserId()).execute();

        // wait for export
        Thread.sleep(45000);

        // fetch synapse tables
        Map<String, String> tableIdToOrderByClause = new HashMap<>();
        // participant version tables
        tableIdToOrderByClause.put(ex3Config.getParticipantVersionTableId(), PARTICIPANT_VERSIONS_TABLE_ORDER_BY);
        tableIdToOrderByClause.put(ex3ConfigForStudy.getParticipantVersionTableId(),
                PARTICIPANT_VERSIONS_TABLE_ORDER_BY);
        tableIdToOrderByClause.put(ex3ConfigForStudy2.getParticipantVersionTableId(),
                PARTICIPANT_VERSIONS_TABLE_ORDER_BY);
        // demographics tables
        tableIdToOrderByClause.put(ex3Config.getParticipantVersionDemographicsTableId(), null);
        tableIdToOrderByClause.put(ex3ConfigForStudy.getParticipantVersionDemographicsTableId(), null);
        tableIdToOrderByClause.put(ex3ConfigForStudy2.getParticipantVersionDemographicsTableId(), null);
        // joined participant versions and demographics tables
        tableIdToOrderByClause.put(ex3Config.getParticipantVersionDemographicsViewId(),
                PARTICIPANT_VERSIONS_DEMOGRAPHICS_VIEW_ORDER_BY);
        tableIdToOrderByClause.put(ex3ConfigForStudy.getParticipantVersionDemographicsViewId(),
                PARTICIPANT_VERSIONS_DEMOGRAPHICS_VIEW_ORDER_BY);
        tableIdToOrderByClause.put(ex3ConfigForStudy2.getParticipantVersionDemographicsViewId(),
                PARTICIPANT_VERSIONS_DEMOGRAPHICS_VIEW_ORDER_BY);
        Map<String, QueryResultBundle> queryResults = selectAllFromSynapseTablesWithQueryCount(tableIdToOrderByClause,
                100L);

        // get study participant versions
        QueryResultBundle studyParticipantVersionTableResults = queryResults
                .get(ex3ConfigForStudy.getParticipantVersionTableId());
        QueryResult studyParticipantVersionTableResult = studyParticipantVersionTableResults.getQueryResult();
        List<Row> studyParticipantVersionRows = studyParticipantVersionTableResult.getQueryResults().getRows();

        // check study demographics table
        // don't need to check the values in the table because they are included in the
        // view
        QueryResultBundle demographicsStudyTableResults = queryResults
                .get(ex3ConfigForStudy.getParticipantVersionDemographicsTableId());
        // check demographics table size
        assertEquals(demographicsStudyTableResults.getQueryCount().longValue(), 12L);
        // check demographics table column names
        verifySynapseTableColumns(demographicsStudyTableResults.getQueryResult(),
                ImmutableList.of("healthCode", "participantVersion", "appId", "studyId", "demographicCategoryName",
                        "demographicValue", "demographicUnits"));

        // check study demographics view
        QueryResultBundle demographicsStudyViewResults = queryResults
                .get(ex3ConfigForStudy.getParticipantVersionDemographicsViewId());
        // check study demographics view size
        assertEquals(demographicsStudyViewResults.getQueryCount().longValue(), 12L);
        QueryResult demographicsStudyViewResult = demographicsStudyViewResults.getQueryResult();
        // check study demographics view column names
        verifySynapseTableColumns(demographicsStudyViewResult,
                ImmutableList.of("healthCode", "participantVersion", "createdOn", "modifiedOn", "dataGroups",
                        "languages", "sharingScope", "studyMemberships", "clientTimeZone", "appId", "studyId",
                        "demographicCategoryName", "demographicValue", "demographicUnits"));
        // check study demographics view values
        List<Row> demographicsStudyViewResultRows = demographicsStudyViewResult.getQueryResults().getRows();
        // version 2
        assertEquals(demographicsStudyViewResultRows.get(0).getValues(), makeExpectedDemographicsViewRow(
                studyParticipantVersionRows, 2, IntegTestUtils.TEST_APP_ID, STUDY_ID, "category0", "value0", null));
        // version 3
        assertEquals(demographicsStudyViewResultRows.get(1).getValues(), makeExpectedDemographicsViewRow(
                studyParticipantVersionRows, 3, IntegTestUtils.TEST_APP_ID, STUDY_ID, "category1", null, null));
        assertEquals(demographicsStudyViewResultRows.get(2).getValues(), makeExpectedDemographicsViewRow(
                studyParticipantVersionRows, 3, IntegTestUtils.TEST_APP_ID, STUDY_ID, "category2", "value1", "units1"));
        // version 4
        assertEquals(demographicsStudyViewResultRows.get(3).getValues(), makeExpectedDemographicsViewRow(
                studyParticipantVersionRows, 4, IntegTestUtils.TEST_APP_ID, STUDY_ID, "category2", "value1", "units1"));
        // version 5
        assertEquals(demographicsStudyViewResultRows.get(4).getValues(), makeExpectedDemographicsViewRow(
                studyParticipantVersionRows, 5, IntegTestUtils.TEST_APP_ID, null, "category3", "value2", null));
        assertEquals(demographicsStudyViewResultRows.get(5).getValues(), makeExpectedDemographicsViewRow(
                studyParticipantVersionRows, 5, IntegTestUtils.TEST_APP_ID, null, "category3", "value3", null));
        assertEquals(demographicsStudyViewResultRows.get(6).getValues(), makeExpectedDemographicsViewRow(
                studyParticipantVersionRows, 5, IntegTestUtils.TEST_APP_ID, STUDY_ID, "category2", "value1", "units1"));
        // version 6
        assertEquals(demographicsStudyViewResultRows.get(7).getValues(), makeExpectedDemographicsViewRow(
                studyParticipantVersionRows, 6, IntegTestUtils.TEST_APP_ID, null, "category3", "value2", null));
        assertEquals(demographicsStudyViewResultRows.get(8).getValues(), makeExpectedDemographicsViewRow(
                studyParticipantVersionRows, 6, IntegTestUtils.TEST_APP_ID, null, "category3", "value3", null));
        assertEquals(demographicsStudyViewResultRows.get(9).getValues(), makeExpectedDemographicsViewRow(
                studyParticipantVersionRows, 6, IntegTestUtils.TEST_APP_ID, STUDY_ID, "category2", "value1", "units1"));
        // version 7
        assertEquals(demographicsStudyViewResultRows.get(10).getValues(), makeExpectedDemographicsViewRow(
                studyParticipantVersionRows, 7, IntegTestUtils.TEST_APP_ID, null, "category3", "value2", null));
        assertEquals(demographicsStudyViewResultRows.get(11).getValues(), makeExpectedDemographicsViewRow(
                studyParticipantVersionRows, 7, IntegTestUtils.TEST_APP_ID, null, "category3", "value3", null));

        // get app participant versions
        QueryResultBundle appParticipantVersionTableResults = queryResults
                .get(ex3Config.getParticipantVersionTableId());
        QueryResult appParticipantVersionTableResult = appParticipantVersionTableResults.getQueryResult();
        List<Row> appParticipantVersionRows = appParticipantVersionTableResult.getQueryResults().getRows();

        // check app demographics table
        // don't need to check the values in the table because they are included in the
        // view
        QueryResultBundle demographicsAppTableResults = queryResults
                .get(ex3Config.getParticipantVersionDemographicsTableId());
        // check demographics table size
        assertEquals(demographicsAppTableResults.getQueryCount().longValue(), 12L);
        verifySynapseTableColumns(demographicsAppTableResults.getQueryResult(),
                ImmutableList.of("healthCode", "participantVersion", "appId", "studyId", "demographicCategoryName",
                        "demographicValue", "demographicUnits"));

        // check app demographics view
        QueryResultBundle demographicsAppViewResults = queryResults
                .get(ex3Config.getParticipantVersionDemographicsViewId());
        // check app demographics view size
        assertEquals(demographicsAppViewResults.getQueryCount().longValue(), 12L);
        QueryResult demographicsAppViewResult = demographicsAppViewResults.getQueryResult();
        // check demographics view column names
        verifySynapseTableColumns(demographicsAppViewResult,
                ImmutableList.of("healthCode", "participantVersion", "createdOn", "modifiedOn", "dataGroups",
                        "languages", "sharingScope", "studyMemberships", "clientTimeZone", "appId", "studyId",
                        "demographicCategoryName", "demographicValue", "demographicUnits"));
        // check app demographics view values
        List<Row> demographicsAppViewResultRows = demographicsAppViewResult.getQueryResults().getRows();
        // version 2
        assertEquals(demographicsAppViewResultRows.get(0).getValues(), makeExpectedDemographicsViewRow(
                appParticipantVersionRows, 2, IntegTestUtils.TEST_APP_ID, STUDY_ID, "category0", "value0", null));
        // version 3
        assertEquals(demographicsAppViewResultRows.get(1).getValues(), makeExpectedDemographicsViewRow(
                appParticipantVersionRows, 3, IntegTestUtils.TEST_APP_ID, STUDY_ID, "category1", null, null));
        assertEquals(demographicsAppViewResultRows.get(2).getValues(), makeExpectedDemographicsViewRow(
                appParticipantVersionRows, 3, IntegTestUtils.TEST_APP_ID, STUDY_ID, "category2", "value1", "units1"));
        // version 4
        assertEquals(demographicsAppViewResultRows.get(3).getValues(), makeExpectedDemographicsViewRow(
                appParticipantVersionRows, 4, IntegTestUtils.TEST_APP_ID, STUDY_ID, "category2", "value1", "units1"));
        // version 5
        assertEquals(demographicsAppViewResultRows.get(4).getValues(), makeExpectedDemographicsViewRow(
                appParticipantVersionRows, 5, IntegTestUtils.TEST_APP_ID, null, "category3", "value2", null));
        assertEquals(demographicsAppViewResultRows.get(5).getValues(), makeExpectedDemographicsViewRow(
                appParticipantVersionRows, 5, IntegTestUtils.TEST_APP_ID, null, "category3", "value3", null));
        assertEquals(demographicsAppViewResultRows.get(6).getValues(), makeExpectedDemographicsViewRow(
                appParticipantVersionRows, 5, IntegTestUtils.TEST_APP_ID, STUDY_ID, "category2", "value1", "units1"));
        // version 6
        assertEquals(demographicsAppViewResultRows.get(7).getValues(), makeExpectedDemographicsViewRow(
                appParticipantVersionRows, 6, IntegTestUtils.TEST_APP_ID, null, "category3", "value2", null));
        assertEquals(demographicsAppViewResultRows.get(8).getValues(), makeExpectedDemographicsViewRow(
                appParticipantVersionRows, 6, IntegTestUtils.TEST_APP_ID, null, "category3", "value3", null));
        assertEquals(demographicsAppViewResultRows.get(9).getValues(), makeExpectedDemographicsViewRow(
                appParticipantVersionRows, 6, IntegTestUtils.TEST_APP_ID, STUDY_ID, "category2", "value1", "units1"));
        // version 7
        assertEquals(demographicsAppViewResultRows.get(10).getValues(), makeExpectedDemographicsViewRow(
                appParticipantVersionRows, 7, IntegTestUtils.TEST_APP_ID, null, "category3", "value2", null));
        assertEquals(demographicsAppViewResultRows.get(11).getValues(), makeExpectedDemographicsViewRow(
                appParticipantVersionRows, 7, IntegTestUtils.TEST_APP_ID, null, "category3", "value3", null));

        // get study2 participant versions
        QueryResultBundle study2ParticipantVersionTableResults = queryResults
                .get(ex3ConfigForStudy2.getParticipantVersionTableId());
        QueryResult study2ParticipantVersionTableResult = study2ParticipantVersionTableResults.getQueryResult();
        List<Row> study2ParticipantVersionRows = study2ParticipantVersionTableResult.getQueryResults().getRows();

        // check study2 demographics table
        // don't need to check the values in the table because they are included in the
        // view
        QueryResultBundle demographicsStudy2TableResults = queryResults
                .get(ex3ConfigForStudy2.getParticipantVersionDemographicsTableId());
        // check demographics table size
        assertEquals(demographicsStudy2TableResults.getQueryCount().longValue(), 6L);
        verifySynapseTableColumns(demographicsStudy2TableResults.getQueryResult(),
                ImmutableList.of("healthCode", "participantVersion", "appId", "studyId", "demographicCategoryName",
                        "demographicValue", "demographicUnits"));

        // check study2 demographics view
        QueryResultBundle demographicsStudy2ViewResults = queryResults
                .get(ex3ConfigForStudy2.getParticipantVersionDemographicsViewId());
        // check app demographics view size
        assertEquals(demographicsStudy2ViewResults.getQueryCount().longValue(), 6L);
        QueryResult demographicsStudy2ViewResult = demographicsStudy2ViewResults.getQueryResult();
        // check demographics view column names
        verifySynapseTableColumns(demographicsStudy2ViewResult,
                ImmutableList.of("healthCode", "participantVersion", "createdOn", "modifiedOn", "dataGroups",
                        "languages", "sharingScope", "studyMemberships", "clientTimeZone", "appId", "studyId",
                        "demographicCategoryName", "demographicValue", "demographicUnits"));
        // check demographics view values
        // should only have app demographics (study demographics should not be in study2
        // view)
        List<Row> demographicsStudy2ViewResultRows = demographicsStudy2ViewResult.getQueryResults().getRows();
        // version 5
        assertEquals(demographicsStudy2ViewResultRows.get(0).getValues(), makeExpectedDemographicsViewRow(
                study2ParticipantVersionRows, 5, IntegTestUtils.TEST_APP_ID, null, "category3", "value2", null));
        assertEquals(demographicsStudy2ViewResultRows.get(1).getValues(), makeExpectedDemographicsViewRow(
                study2ParticipantVersionRows, 5, IntegTestUtils.TEST_APP_ID, null, "category3", "value3", null));
        // version 6
        assertEquals(demographicsStudy2ViewResultRows.get(2).getValues(), makeExpectedDemographicsViewRow(
                study2ParticipantVersionRows, 6, IntegTestUtils.TEST_APP_ID, null, "category3", "value2", null));
        assertEquals(demographicsStudy2ViewResultRows.get(3).getValues(), makeExpectedDemographicsViewRow(
                study2ParticipantVersionRows, 6, IntegTestUtils.TEST_APP_ID, null, "category3", "value3", null));
        // version 7
        assertEquals(demographicsStudy2ViewResultRows.get(4).getValues(), makeExpectedDemographicsViewRow(
                study2ParticipantVersionRows, 7, IntegTestUtils.TEST_APP_ID, null, "category3", "value2", null));
        assertEquals(demographicsStudy2ViewResultRows.get(5).getValues(), makeExpectedDemographicsViewRow(
                study2ParticipantVersionRows, 7, IntegTestUtils.TEST_APP_ID, null, "category3", "value3", null));
    }

    private void verifySynapseTableColumns(QueryResult queryResult, List<String> expectedColumnNames) {
        assertEquals(queryResult.getQueryResults().getHeaders().size(), expectedColumnNames.size());
        queryResult.getQueryResults().getHeaders().stream().map(column -> column.getName());
        for (int i = 0; i < expectedColumnNames.size(); i++) {
            assertEquals(queryResult.getQueryResults().getHeaders().get(i).getName(), expectedColumnNames.get(i));
        }
    }

    private List<String> makeExpectedDemographicsViewRow(List<Row> participantVersionRows, int versionNum, String appId,
            String studyId, String categoryName, String value, String units) {
        Row participantVersionRow = null;
        for (Row row : participantVersionRows) {
            // version num is at column index 1 in participant versions table
            if (row.getValues().get(1).equals(String.valueOf(versionNum))) {
                participantVersionRow = row;
            }
        }
        if (participantVersionRow == null) {
            throw new IllegalArgumentException(
                    "the specified version " + versionNum + " does not exist in the participant versions table");
        }
        // the first part of each row is the same as in the participant versions table
        List<String> expectedRow = new ArrayList<>(participantVersionRow.getValues());
        expectedRow.add(appId);
        expectedRow.add(studyId);
        expectedRow.add(categoryName);
        expectedRow.add(value);
        expectedRow.add(units);
        return expectedRow;
    }

    /**
     * Requests multiple tables from Synapse simultaneously with async queries.
     * 
     * @param tableIdToOrderByClause A map of table ids that should be fetched to an
     *                               order by clause for ordering the query results.
     *                               The order by clause can be null. The order by
     *                               clause should be a string containing column
     *                               names separated by a comma and a space.
     * @param limit                  The limit for number of rows.
     * @return A map of table ids to the results for that table. The results contain
     *         the count of the query result and the query results themselves.
     * @throws SynapseException
     * @throws InterruptedException
     * @throws TimeoutException
     */
    private Map<String, QueryResultBundle> selectAllFromSynapseTablesWithQueryCount(
            Map<String, String> tableIdToOrderByClause, long limit)
            throws SynapseException, InterruptedException, TimeoutException {
        Map<String, String> tableIdToJobId = new HashMap<>(tableIdToOrderByClause.size());
        for (Map.Entry<String, String> entry : tableIdToOrderByClause.entrySet()) {
            String tableId = entry.getKey();
            String orderByClause = entry.getValue();
            String query = "select * from " + tableId;
            if (orderByClause != null) {
                query += " order by " + orderByClause;
            }
            String jobId = synapseClient.queryTableEntityBundleAsyncStart(query, 0L, limit,
                    SynapseClient.QUERY_PARTMASK | SynapseClient.COUNT_PARTMASK, tableId);
            tableIdToJobId.put(tableId, jobId);
        }
        Map<String, QueryResultBundle> queryResults = new HashMap<>(tableIdToOrderByClause.size());
        for (int retries = 0; retries < 20; retries++) {
            for (Iterator<Map.Entry<String, String>> iter = tableIdToJobId.entrySet().iterator(); iter.hasNext();) {
                Map.Entry<String, String> entry = iter.next();
                String tableId = entry.getKey();
                String jobId = entry.getValue();
                try {
                    QueryResultBundle queryResult = synapseClient.queryTableEntityBundleAsyncGet(jobId, tableId);
                    // if we get here the query is complete
                    queryResults.put(tableId, queryResult);
                    // remove it so we don't try to poll it again
                    iter.remove();
                } catch (SynapseResultNotReadyException e) {

                }

                if (tableIdToJobId.isEmpty()) {
                    // no more jobs left to wait on
                    return queryResults;
                }
                Thread.sleep(500);
            }
        }
        throw new TimeoutException("timed out waiting for Synapse query results");
    }

    private void testUpload(byte[] content, boolean encrypted) throws Exception {
        testUpload(content, encrypted,
                ImmutableMap.of(CUSTOM_METADATA_KEY, CUSTOM_METADATA_VALUE),
                ImmutableMap.of(CUSTOM_METADATA_KEY_SANITIZED, CUSTOM_METADATA_VALUE));
    }
    
    private void testUpload(byte[] content, boolean encrypted, Map<String,String> userMetadata, Map<String,String> expectedMetadata) throws Exception {
        // Participants created by TestUserHelper (UserAdminService) are set to no_sharing by default. Enable sharing
        // so that the test can succeed.
        ParticipantsApi participantsApi = user.getClient(ParticipantsApi.class);
        StudyParticipant participant = participantsApi.getUsersParticipantRecord(false).execute().body();
        participant.setSharingScope(SharingScope.ALL_QUALIFIED_RESEARCHERS);
        participantsApi.updateUsersParticipantRecord(participant).execute();

        // Upload file to Bridge.
        UploadInfo uploadInfo = uploadFile(content, encrypted, userMetadata, expectedMetadata);
        String filename = uploadInfo.filename;
        String uploadId = uploadInfo.uploadId;

        // Verify Synapse and S3.
        verifyUpload(ex3Config, uploadId, filename, false, expectedMetadata);
        verifyUpload(ex3ConfigForStudy, uploadId, filename, true, expectedMetadata);

        // Verify the record in Bridge.
        ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);
        HealthDataRecordEx3 record = usersApi.getRecordEx3ById(uploadId, "true").execute().body();
        assertTrue(record.isExported());
        DateTime oneHourAgo = DateTime.now().minusHours(1);
        assertTrue(record.getExportedOn().isAfter(oneHourAgo));

        // Verify the presigned download url for a health record is generated and contains the data expected.
        String url = record.getDownloadUrl();
        HttpResponse responseForPresignedUrl = Request.Get(url).execute().returnResponse();
        String contentForPresignedUrl = EntityUtils.toString(responseForPresignedUrl.getEntity());
        assertEquals(200, responseForPresignedUrl.getStatusLine().getStatusCode());
        assertEquals(UPLOAD_CONTENT, contentForPresignedUrl.getBytes(StandardCharsets.UTF_8));

        DateTime fiftyMinsAfter = DateTime.now().plusMinutes(50);
        assertTrue(record.getDownloadExpiration().isAfter(fiftyMinsAfter));
    }

    private void verifyUpload(Exporter3Configuration ex3Config, String uploadId, String filename, boolean isForStudy,
            Map<String,String> expectedMetadata) throws Exception {
        // Verify Synapse file entity and annotations.
        String rawFolderId = ex3Config.getRawDataFolderId();
        String todaysDateString = LocalDate.now(TestUtils.LOCAL_TIME_ZONE).toString();
        String exportedFilename = uploadId + '-' + filename;

        // First, get the exported file.
        String todayFolderId = getSynapseChildByName(rawFolderId, todaysDateString);
        String exportedFileId = getSynapseChildByName(todayFolderId, exportedFilename);

        // Now verify the annotations.
        Annotations annotations = synapseClient.getAnnotationsV2(exportedFileId);
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
        assertEquals(s3Bytes, UPLOAD_CONTENT);

        ObjectMetadata s3Metadata = s3Helper.getObjectMetadata(rawDataBucket, expectedS3Key);
        assertEquals(s3Metadata.getSSEAlgorithm(), ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        assertEquals(s3Metadata.getContentType(), CONTENT_TYPE_TEXT_PLAIN);
        verifyMetadata(s3Metadata.getUserMetadata(), uploadId, expectedMetadata);

        // Verify the Participant Version table.
        String healthCode = flattenedAnnotationMap.get("healthCode");
        String participantVersionStr = flattenedAnnotationMap.get("participantVersion");
        RowSet queryRowSet = queryParticipantVersion(ex3Config, healthCode, participantVersionStr);
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
    }

    private RowSet queryParticipantVersion(Exporter3Configuration ex3Config, String healthCode,
            String participantVersionStr) throws Exception {
        // Query participant version table.
        String participantVersionTableId = ex3Config.getParticipantVersionTableId();
        String query = "SELECT * FROM " + participantVersionTableId + " WHERE healthCode='" + healthCode + "'";
        if (participantVersionStr != null) {
            query += " and participantVersion=" + participantVersionStr;
        }
        String queryJobId = synapseClient.queryTableEntityBundleAsyncStart(query, null, null,
                SynapseClient.QUERY_PARTMASK, participantVersionTableId);

        QueryResultBundle queryResultBundle = null;
        for (int loops = 0; loops < 5; loops++) {
            // Poll until result is ready.
            Thread.sleep(1000);
            try {
                queryResultBundle = synapseClient.queryTableEntityBundleAsyncGet(queryJobId,
                        participantVersionTableId);
            } catch (SynapseResultNotReadyException ex) {
                // Wait and try again.
            }
        }
        assertNotNull(queryResultBundle, "Timed out querying for participant version");

        return queryResultBundle.getQueryResult().getQueryResults();
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
            return synapseClient.lookupChild(parentId, childName);
        } catch (SynapseNotFoundException ex) {
            return null;
        }
    }

    private static void verifyMetadata(Map<String, String> metadataMap, String expectedRecordId, Map<String,String> expectedValues)
    throws Exception {
        assertEquals(metadataMap.size(), 7 + expectedValues.size());
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

        // Timestamps are relatively recent. Because of clock skew on Jenkins, give a very generous time window of,
        // let's say, 1 hour.
        DateTime oneHourAgo = DateTime.now().minusHours(1);
        DateTime exportedOn = DateTime.parse(metadataMap.get("exportedOn"));
        assertTrue(exportedOn.isAfter(oneHourAgo));

        DateTime uploadedOn = DateTime.parse(metadataMap.get("uploadedOn"));
        assertTrue(uploadedOn.isAfter(oneHourAgo));
    }
}
