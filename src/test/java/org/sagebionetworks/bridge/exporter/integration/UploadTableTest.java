package org.sagebionetworks.bridge.exporter.integration;

import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_ID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import au.com.bytecode.opencsv.CSVReader;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3Client;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.api.AppsApi;
import org.sagebionetworks.bridge.rest.api.AssessmentsApi;
import org.sagebionetworks.bridge.rest.api.ForSuperadminsApi;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.api.ParticipantsApi;
import org.sagebionetworks.bridge.rest.api.SharedAssessmentsApi;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.api.UploadsApi;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.Assessment;
import org.sagebionetworks.bridge.rest.model.Enrollment;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.UploadTableJob;
import org.sagebionetworks.bridge.rest.model.UploadTableJobResult;
import org.sagebionetworks.bridge.rest.model.UploadTableJobStatus;
import org.sagebionetworks.bridge.rest.model.UploadTableRow;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.SqsHelper;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;

@SuppressWarnings("deprecation")
public class UploadTableTest {
    private static final Logger LOG = LoggerFactory.getLogger(UploadTableTest.class);

    private static final String ASSESSMENT_A_TITLE = "Assessment A: First Assessment";
    private static final String ASSESSMENT_A_TITLE_TRIMMED = "AssessmentAFirstAssessment";
    private static final String ASSESSMENT_B_TITLE = "Assessment B, w/ Special Characters?!";
    private static final String ASSESSMENT_B_TITLE_TRIMMED = "AssessmentBwSpecialCharacters";
    private static final String CONFIG_KEY_RAW_DATA_BUCKET = "health.data.bucket.raw";
    private static final DateTime CREATED_ON_1A = DateTime.parse("2017-05-10T06:47:33.701Z");
    private static final DateTime CREATED_ON_1B = DateTime.parse("2017-05-11T10:36:36.638Z");
    private static final DateTime CREATED_ON_2A = DateTime.parse("2017-05-12T16:40:05.089Z");
    private static final DateTime CREATED_ON_2B = DateTime.parse("2017-05-13T18:06:36.803Z");
    private static final String RECORD_ID_1A = "record-id-1a";
    private static final String RECORD_ID_1B = "record-id-1b";
    private static final String RECORD_ID_2A = "record-id-2a";
    private static final String RECORD_ID_2B = "record-id-2b";
    private static final String STUDY_NAME = "Upload Table: Test Study";
    private static final String STUDY_NAME_TRIMMED = "UploadTableTestStudy";
    static final String WORKER_ID_CSV_WORKER = "UploadCsvWorker";

    private static final String[] COMMON_COLUMNS = {
            "recordId",
            "studyId",
            "studyName",
            "assessmentGuid",
            "assessmentId",
            "assessmentRevision",
            "assessmentTitle",
            "createdOn",
            "isTestData",
            "healthCode",
            "participantVersion",
    };

    private static class JobGuidAndFiles {
        private final String jobGuid;
        private final Map<String, File> filesByName;

        public JobGuidAndFiles(String jobGuid, Map<String, File> filesByName) {
            this.jobGuid = jobGuid;
            this.filesByName = filesByName;
        }

        public String getJobGuid() {
            return jobGuid;
        }

        public Map<String, File> getFilesByName() {
            return filesByName;
        }
    }

    private static TestUser admin;
    private static String assessmentGuidA;
    private static String assessmentGuidB;
    private static String assessmentIdA;
    private static String assessmentIdB;
    private static Table ddbWorkerLogTable;
    private static StudyParticipant participant1;
    private static StudyParticipant participant2;
    private static String rawDataBucket;
    private static TestUser researcher;
    private static S3Helper s3Helper;
    private static String sharedAssessmentGuid;
    private static SqsHelper sqsHelper;
    private static String studyId;
    private static TestUser user1;
    private static TestUser user2;
    private static TestUser worker;
    private static String workerSqsUrl;

    private Set<String> recordIdsToDelete;

    @BeforeClass
    public static void beforeClass() throws IOException {
        Config config = TestUtils.loadConfig();
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

        // Don't need to init exporter 3, but we do need to enable it in the app.
        admin = TestUserHelper.getSignedInAdmin();
        AppsApi appsApi = admin.getClient(AppsApi.class);
        App app = appsApi.getUsersApp().execute().body();
        app.setExporter3Enabled(true);
        appsApi.updateUsersApp(app).execute();

        // Create study.
        studyId = TestUtils.randomIdentifier(UploadTableTest.class);
        Study study = new Study().identifier(studyId).name(STUDY_NAME);
        StudiesApi studiesApi = admin.getClient(StudiesApi.class);
        studiesApi.createStudy(study).execute();

        // Create 2 assessments.
        AssessmentsApi assessmentsApi = admin.getClient(AssessmentsApi.class);

        assessmentIdA = TestUtils.randomIdentifier(UploadTableTest.class);
        Assessment assessmentA = new Assessment().title(ASSESSMENT_A_TITLE).osName("Universal")
                .ownerId("sage-bionetworks").identifier(assessmentIdA).phase(Assessment.PhaseEnum.DRAFT);
        assessmentA = assessmentsApi.createAssessment(assessmentA).execute().body();
        assessmentGuidA = assessmentA.getGuid();

        assessmentIdB = TestUtils.randomIdentifier(UploadTableTest.class);
        Assessment assessmentB = new Assessment().title(ASSESSMENT_B_TITLE).osName("Universal")
                .ownerId("sage-bionetworks").identifier(assessmentIdB).phase(Assessment.PhaseEnum.DRAFT);
        assessmentB = assessmentsApi.createAssessment(assessmentB).execute().body();
        assessmentGuidB = assessmentB.getGuid();

        // Publish one of these assessments into a shared assessment.
        assessmentsApi.publishAssessment(assessmentGuidA, assessmentIdA).execute();
        Assessment sharedAssessment = admin.getClient(SharedAssessmentsApi.class).getLatestSharedAssessmentRevision(
                assessmentIdA).execute().body();
        sharedAssessmentGuid = sharedAssessment.getGuid();

        // Create researcher, worker, and participants.
        researcher = TestUserHelper.createAndSignInUser(UploadTableTest.class, true, Role.RESEARCHER);
        worker = TestUserHelper.createAndSignInUser(UploadTableTest.class, true, Role.WORKER);
        user1 = TestUserHelper.createAndSignInUser(UploadTableTest.class, true);
        user2 = TestUserHelper.createAndSignInUser(UploadTableTest.class, true);

        // Enroll the participants in the study. (External ID is optional.)
        Enrollment enrollment1 = new Enrollment().userId(user1.getUserId());
        studiesApi.enrollParticipant(studyId, enrollment1).execute();

        Enrollment enrollment2 = new Enrollment().userId(user2.getUserId());
        studiesApi.enrollParticipant(studyId, enrollment2).execute();

        ParticipantsApi participantsApi = admin.getClient(ParticipantsApi.class);
        participant1 = participantsApi.getParticipantById(user1.getUserId(), false).execute().body();
        participant2 = participantsApi.getParticipantById(user2.getUserId(), false).execute().body();
    }

    @BeforeMethod
    public void before() {
        // Reset record IDs to delete.
        recordIdsToDelete = new HashSet<>();
    }

    @AfterMethod
    public void after() throws IOException {
        // Delete upload table rows.
        ForSuperadminsApi superadminsApi = admin.getClient(ForSuperadminsApi.class);
        for (String oneRecordId : recordIdsToDelete) {
            superadminsApi.deleteUploadTableRowForSuperadmin(TEST_APP_ID, studyId, oneRecordId).execute();
        }
    }

    @AfterClass
    public static void afterClass() throws IOException {
        // Delete test researcher, worker, and users.
        if (researcher != null) {
            researcher.signOutAndDeleteUser();
        }
        if (worker != null) {
            worker.signOutAndDeleteUser();
        }
        if (user1 != null) {
            user1.signOutAndDeleteUser();
        }
        if (user2 != null) {
            user2.signOutAndDeleteUser();
        }

        // Delete assessments.
        if (assessmentGuidA != null) {
            admin.getClient(AssessmentsApi.class).deleteAssessment(assessmentGuidA, true).execute();
        }
        if (assessmentGuidB != null) {
            admin.getClient(AssessmentsApi.class).deleteAssessment(assessmentGuidB, true).execute();
        }
        if (sharedAssessmentGuid != null) {
            admin.getClient(SharedAssessmentsApi.class).deleteSharedAssessment(sharedAssessmentGuid, true)
                    .execute();
        }

        // Delete the test study. This automatically deletes all table rows in that study.
        if (studyId != null) {
            StudiesApi studiesApi = admin.getClient(StudiesApi.class);
            studiesApi.deleteStudy(studyId, true).execute();
        }

        // Disable exporter 3 to restore the app to its original state.
        AppsApi appsApi = admin.getClient(AppsApi.class);
        App app = appsApi.getUsersApp().execute().body();
        app.setExporter3Enabled(false);
        appsApi.updateUsersApp(app).execute();
    }

    @Test
    public void allAssessmentsForStudy() throws Exception {
        // Create 4 table rows, one for each participant and assessment.
        UploadTableRow row1a = new UploadTableRow().recordId(RECORD_ID_1A).assessmentGuid(assessmentGuidA)
                .createdOn(CREATED_ON_1A).testData(true).healthCode(participant1.getHealthCode()).participantVersion(1)
                .putMetadataItem("foo", "metadata1a").putDataItem("bar", "data1a");
        createUploadTableRow(row1a);

        UploadTableRow row1b = new UploadTableRow().recordId(RECORD_ID_1B).assessmentGuid(assessmentGuidB)
                .createdOn(CREATED_ON_1B).testData(true).healthCode(participant1.getHealthCode()).participantVersion(1)
                .putMetadataItem("foo", "metadata1b").putDataItem("bar", "data1b");
        createUploadTableRow(row1b);

        UploadTableRow row2a = new UploadTableRow().recordId(RECORD_ID_2A).assessmentGuid(assessmentGuidA)
                .createdOn(CREATED_ON_2A).testData(true).healthCode(participant2.getHealthCode()).participantVersion(1)
                .putMetadataItem("foo", "metadata2a").putDataItem("bar", "data2a");
        createUploadTableRow(row2a);

        UploadTableRow row2b = new UploadTableRow().recordId(RECORD_ID_2B).assessmentGuid(assessmentGuidB)
                .createdOn(CREATED_ON_2B).testData(true).healthCode(participant2.getHealthCode()).participantVersion(1)
                .putMetadataItem("foo", "metadata2b").putDataItem("bar", "data2b");
        createUploadTableRow(row2b);

        // Run worker.
        JobGuidAndFiles jobGuidAndFiles = runWorker();
        String jobGuid = jobGuidAndFiles.getJobGuid();
        Map<String, File> filesByName = jobGuidAndFiles.getFilesByName();

        // Verify files.
        assertEquals(filesByName.size(), 2);
        String expectedFilenameA = studyId + "-" + STUDY_NAME_TRIMMED + "-" + assessmentGuidA + "-" +
                ASSESSMENT_A_TITLE_TRIMMED + ".csv";
        String expectedFilenameB = studyId + "-" + STUDY_NAME_TRIMMED + "-" + assessmentGuidB + "-" +
                ASSESSMENT_B_TITLE_TRIMMED + ".csv";
        assertTrue(filesByName.containsKey(expectedFilenameA), "Missing file: " + expectedFilenameA);
        assertTrue(filesByName.containsKey(expectedFilenameB), "Missing file: " + expectedFilenameB);

        String[] additionalHeaders = new String[] { "foo", "bar" };

        File fileA = filesByName.get(expectedFilenameA);
        Map<String, String[]> expectedRowsByRecordIdA = new HashMap<>();
        expectedRowsByRecordIdA.put(RECORD_ID_1A, new String[] { RECORD_ID_1A, studyId, STUDY_NAME, assessmentGuidA,
                assessmentIdA, "1", ASSESSMENT_A_TITLE, CREATED_ON_1A.toString(), "true", participant1.getHealthCode(),
                "1", "metadata1a", "data1a" });
        expectedRowsByRecordIdA.put(RECORD_ID_2A, new String[] { RECORD_ID_2A, studyId, STUDY_NAME, assessmentGuidA,
                assessmentIdA, "1", ASSESSMENT_A_TITLE, CREATED_ON_2A.toString(), "true", participant2.getHealthCode(),
                "1", "metadata2a", "data2a" });
        assertCsvFile(fileA, additionalHeaders, expectedRowsByRecordIdA);

        File fileB = filesByName.get(expectedFilenameB);
        Map<String, String[]> expectedRowsByRecordIdB = new HashMap<>();
        expectedRowsByRecordIdB.put(RECORD_ID_1B, new String[] { RECORD_ID_1B, studyId, STUDY_NAME, assessmentGuidB,
                assessmentIdB, "1", ASSESSMENT_B_TITLE, CREATED_ON_1B.toString(), "true", participant1.getHealthCode(),
                "1", "metadata1b", "data1b" });
        expectedRowsByRecordIdB.put(RECORD_ID_2B, new String[] { RECORD_ID_2B, studyId, STUDY_NAME, assessmentGuidB,
                assessmentIdB, "1", ASSESSMENT_B_TITLE, CREATED_ON_2B.toString(), "true", participant2.getHealthCode(),
                "1", "metadata2b", "data2b" });
        assertCsvFile(fileB, additionalHeaders, expectedRowsByRecordIdB);

        // Verify table job APIs. We've only had one job in this study, so the most recent one should have a matching
        // job GUID.
        List<UploadTableJob> jobList = researcher.getClient(UploadsApi.class).listUploadTableJobsForStudy(studyId,
                0, 5).execute().body().getItems();
        assertEquals(jobList.size(), 1);
        assertEquals(jobList.get(0).getJobGuid(), jobGuid);

        ForWorkersApi workerApi = worker.getClient(ForWorkersApi.class);
        UploadTableJob job = workerApi.getUploadTableJobForWorker(TEST_APP_ID, studyId, jobGuid).execute().body();
        assertEquals(job.getStatus(), UploadTableJobStatus.SUCCEEDED);

        // Just for fun, update the table job, to verify that the APIs work.
        job.setStatus(UploadTableJobStatus.FAILED);
        workerApi.updateUploadTableJobForWorker(TEST_APP_ID, studyId, jobGuid, job).execute();

        job = workerApi.getUploadTableJobForWorker(TEST_APP_ID, studyId, jobGuid).execute().body();
        assertEquals(job.getStatus(), UploadTableJobStatus.FAILED);
    }

    @Test
    public void assessmentList() throws Exception {
        // This is a simpler test. One participant, two assessments, but only one assessment is requested.
        UploadTableRow row1a = new UploadTableRow().recordId(RECORD_ID_1A).assessmentGuid(assessmentGuidA)
                .createdOn(CREATED_ON_1A).testData(true).healthCode(participant1.getHealthCode()).participantVersion(1)
                .putMetadataItem("foo", "metadata1a").putDataItem("bar", "data1a");
        createUploadTableRow(row1a);

        UploadTableRow row1b = new UploadTableRow().recordId(RECORD_ID_1B).assessmentGuid(assessmentGuidB)
                .createdOn(CREATED_ON_1B).testData(true).healthCode(participant1.getHealthCode()).participantVersion(1)
                .putMetadataItem("foo", "metadata1b").putDataItem("bar", "data1b");
        createUploadTableRow(row1b);

        // Run worker.
        String zipFileSuffix = TestUtils.randomIdentifier(UploadTableTest.class);
        Map<String, File> filesByName = runWorkerWithAssessmentGuids(ImmutableSet.of(assessmentGuidA), zipFileSuffix);

        // Verify files.
        assertEquals(filesByName.size(), 1);
        String expectedFilenameA = studyId + "-" + STUDY_NAME_TRIMMED + "-" + assessmentGuidA + "-" +
                ASSESSMENT_A_TITLE_TRIMMED + ".csv";
        assertTrue(filesByName.containsKey(expectedFilenameA), "Missing file: " + expectedFilenameA);

        String[] additionalHeaders = new String[] { "foo", "bar" };

        File fileA = filesByName.get(expectedFilenameA);
        Map<String, String[]> expectedRowsByRecordIdA = new HashMap<>();
        expectedRowsByRecordIdA.put(RECORD_ID_1A, new String[] { RECORD_ID_1A, studyId, STUDY_NAME, assessmentGuidA,
                assessmentIdA, "1", ASSESSMENT_A_TITLE, CREATED_ON_1A.toString(), "true", participant1.getHealthCode(),
                "1", "metadata1a", "data1a" });
        assertCsvFile(fileA, additionalHeaders, expectedRowsByRecordIdA);
    }

    @Test
    public void columnsTest() throws Exception {
        // Each assessment row has a different set of columns. This tests that the CSVs are generated correctly.
        UploadTableRow row1a = new UploadTableRow().recordId(RECORD_ID_1A).assessmentGuid(assessmentGuidA)
                .createdOn(CREATED_ON_1A).testData(true).healthCode(participant1.getHealthCode()).participantVersion(1)
                .putMetadataItem("A", "A-1a").putMetadataItem("B", "B-1a")
                .putDataItem("X", "X-1a").putDataItem("Y", "Y-1a");
        createUploadTableRow(row1a);

        UploadTableRow row2a = new UploadTableRow().recordId(RECORD_ID_2A).assessmentGuid(assessmentGuidA)
                .createdOn(CREATED_ON_2A).testData(true).healthCode(participant2.getHealthCode()).participantVersion(1)
                .putMetadataItem("A", "A-2a").putMetadataItem("C", "C-2a")
                .putDataItem("X", "X-2a").putDataItem("Z", "Z-2a");
        createUploadTableRow(row2a);

        // Run worker.
        String zipFileSuffix = TestUtils.randomIdentifier(UploadTableTest.class);
        Map<String, File> filesByName = runWorkerWithAssessmentGuids(ImmutableSet.of(assessmentGuidA), zipFileSuffix);

        // Verify files.
        assertEquals(filesByName.size(), 1);
        String expectedFilenameA = studyId + "-" + STUDY_NAME_TRIMMED + "-" + assessmentGuidA + "-" +
                ASSESSMENT_A_TITLE_TRIMMED + ".csv";
        assertTrue(filesByName.containsKey(expectedFilenameA), "Missing file: " + expectedFilenameA);

        String[] additionalHeaders = new String[] { "A", "B", "C", "X", "Y", "Z" };

        File fileA = filesByName.get(expectedFilenameA);
        Map<String, String[]> expectedRowsByRecordIdA = new HashMap<>();
        expectedRowsByRecordIdA.put(RECORD_ID_1A, new String[] { RECORD_ID_1A, studyId, STUDY_NAME, assessmentGuidA,
                assessmentIdA, "1", ASSESSMENT_A_TITLE, CREATED_ON_1A.toString(), "true", participant1.getHealthCode(),
                "1", "A-1a", "B-1a", "", "X-1a", "Y-1a", "" });
        expectedRowsByRecordIdA.put(RECORD_ID_2A, new String[] { RECORD_ID_2A, studyId, STUDY_NAME, assessmentGuidA,
                assessmentIdA, "1", ASSESSMENT_A_TITLE, CREATED_ON_2A.toString(), "true", participant2.getHealthCode(),
                "1", "A-2a", "", "C-2a", "X-2a", "", "Z-2a" });
        assertCsvFile(fileA, additionalHeaders, expectedRowsByRecordIdA);
    }

    @Test
    public void sharedAssessment() throws Exception {
        UploadTableRow row1a = new UploadTableRow().recordId(RECORD_ID_1A).assessmentGuid(sharedAssessmentGuid)
                .createdOn(CREATED_ON_1A).testData(true).healthCode(participant1.getHealthCode()).participantVersion(1)
                .putMetadataItem("foo", "metadata1a").putDataItem("bar", "data1a");
        createUploadTableRow(row1a);

        // Run worker.
        String zipFileSuffix = TestUtils.randomIdentifier(UploadTableTest.class);
        Map<String, File> filesByName = runWorkerWithAssessmentGuids(ImmutableSet.of(sharedAssessmentGuid),
                zipFileSuffix);

        // Verify files.
        assertEquals(filesByName.size(), 1);
        String expectedFilenameA = studyId + "-" + STUDY_NAME_TRIMMED + "-" + sharedAssessmentGuid + "-" +
                ASSESSMENT_A_TITLE_TRIMMED + ".csv";
        assertTrue(filesByName.containsKey(expectedFilenameA), "Missing file: " + expectedFilenameA);

        String[] additionalHeaders = new String[] { "foo", "bar" };

        File fileA = filesByName.get(expectedFilenameA);
        Map<String, String[]> expectedRowsByRecordIdA = new HashMap<>();
        expectedRowsByRecordIdA.put(RECORD_ID_1A, new String[] { RECORD_ID_1A, studyId, STUDY_NAME, sharedAssessmentGuid,
                assessmentIdA, "1", ASSESSMENT_A_TITLE, CREATED_ON_1A.toString(), "true", participant1.getHealthCode(),
                "1", "metadata1a", "data1a" });
        assertCsvFile(fileA, additionalHeaders, expectedRowsByRecordIdA);
    }

    // Helper method to create a table row and add it to our list to delete.
    private void createUploadTableRow(UploadTableRow row) throws IOException {
        admin.getClient(ForWorkersApi.class).saveUploadTableRowForWorker(TEST_APP_ID, studyId, row).execute();
        recordIdsToDelete.add(row.getRecordId());
    }

    private static JobGuidAndFiles runWorker() throws Exception {
        // We need to know the previous finish time so we can determine when the worker is finished.
        long previousFinishTime = TestUtils.getWorkerLastFinishedTime(ddbWorkerLogTable,
                WORKER_ID_CSV_WORKER);

        // Create request.
        UploadsApi researcherUploadsApi = researcher.getClient(UploadsApi.class);
        String jobGuid = researcherUploadsApi.requestUploadTableForStudy(studyId).execute().body().getJobGuid();

        // Wait until the worker is finished.
        TestUtils.pollWorkerLog(ddbWorkerLogTable, WORKER_ID_CSV_WORKER, previousFinishTime);

        // Get the S3 URL from Bridge Server.
        UploadTableJobResult jobResult = researcherUploadsApi.getUploadTableJobResult(studyId, jobGuid).execute()
                .body();
        assertEquals(jobResult.getStatus(), UploadTableJobStatus.SUCCEEDED);
        assertNotNull(jobResult.getUrl());
        assertNotNull(jobResult.getExpiresOn());

        HttpResponse responseForPresignedUrl = Request.Get(jobResult.getUrl()).execute().returnResponse();
        assertEquals(200, responseForPresignedUrl.getStatusLine().getStatusCode());

        Map<String, File> filesByName;
        try (InputStream inputStream = responseForPresignedUrl.getEntity().getContent()) {
            filesByName = unzipStream(inputStream);
        }

        return new JobGuidAndFiles(jobGuid, filesByName);
    }

    // Runs the worker, downloads the zip file, unzips the zip file, and returns a map of files keyed by the file name.
    // This is the older test harness, which calls the worker directly. This tests features that aren't currently
    // exposed through the API.
    private static Map<String, File> runWorkerWithAssessmentGuids(Set<String> assessmentGuids, String zipFileSuffix)
            throws Exception {
        // We need to know the previous finish time so we can determine when the worker is finished.
        long previousFinishTime = TestUtils.getWorkerLastFinishedTime(ddbWorkerLogTable,
                WORKER_ID_CSV_WORKER);

        // Create request.
        ObjectNode requestBodyNode = DefaultObjectMapper.INSTANCE.createObjectNode();
        requestBodyNode.put("appId", TEST_APP_ID);
        requestBodyNode.put("studyId", studyId);
        if (assessmentGuids != null) {
            requestBodyNode.set("assessmentGuids", DefaultObjectMapper.INSTANCE.valueToTree(assessmentGuids));
        }
        requestBodyNode.put("includeTestData", true);
        requestBodyNode.put("zipFileSuffix", zipFileSuffix);

        ObjectNode requestNode = DefaultObjectMapper.INSTANCE.createObjectNode();
        requestNode.put("service", WORKER_ID_CSV_WORKER);
        requestNode.set("body", requestBodyNode);

        sqsHelper.sendMessageAsJson(workerSqsUrl, requestNode, 0);

        // Wait until the worker is finished.
        TestUtils.pollWorkerLog(ddbWorkerLogTable, WORKER_ID_CSV_WORKER, previousFinishTime);

        // Because we called the worker directly, we don't have a job GUID, so we need to download from S3 directly.
        String zipFilename = studyId + "-" + STUDY_NAME_TRIMMED + "-" + zipFileSuffix + ".zip";
        File zipFile = File.createTempFile(zipFilename, ".zip");
        s3Helper.downloadS3File(rawDataBucket, zipFilename, zipFile);
        LOG.info("Downloaded zip file: " + zipFile.getAbsolutePath() + " (" + zipFile.length() + " bytes)");

        try (FileInputStream fileInputStream = new FileInputStream(zipFile)) {
            return unzipStream(fileInputStream);
        }
    }

    static Map<String, File> unzipStream(InputStream stream) throws IOException {
        Map<String, File> filesByName = new HashMap<>();
        try (ZipInputStream zipInputStream = new ZipInputStream(stream)) {
            ZipEntry zipEntry = zipInputStream.getNextEntry();
            while (zipEntry != null) {
                String zipEntryName = zipEntry.getName();

                // Sanity check: No duplicate file names in our zip file.
                assertFalse(filesByName.containsKey(zipEntryName));

                // Add to map.
                File tempFile = File.createTempFile(zipEntryName, null);
                try (FileOutputStream fileOutputStream = new FileOutputStream(tempFile)) {
                    ByteStreams.copy(zipInputStream, fileOutputStream);
                }
                filesByName.put(zipEntryName, tempFile);

                // Get next file.
                zipEntry = zipInputStream.getNextEntry();
            }
        }
        return filesByName;
    }

    static void assertCsvFile(File csvFile, String[] additionalHeaders,
            Map<String, String[]> expectedRowsByRecordId) throws IOException {
        try (CSVReader csvFileReader = new CSVReader(new FileReader(csvFile))) {
            List<String[]> csvLines = csvFileReader.readAll();
            assertEquals(csvLines.size(), expectedRowsByRecordId.size() + 1);

            // Header row.
            String[] headerRow = csvLines.get(0);
            assertHeaders(headerRow, additionalHeaders);

            // Turn remaining rows into a map by recordId to make it easier to verify. (Record ID is the first column.)
            Map<String, String[]> csvRowMap = new HashMap<>();
            for (int i = 1; i < csvLines.size(); i++) {
                String[] oneCsvRow = csvLines.get(i);
                csvRowMap.put(oneCsvRow[0], oneCsvRow);
            }
            assertEquals(csvRowMap.keySet(), expectedRowsByRecordId.keySet());

            // Verify rows.
            for (String recordId : expectedRowsByRecordId.keySet()) {
                assertRow(csvRowMap.get(recordId), expectedRowsByRecordId.get(recordId));
            }
        }
    }

    private static void assertHeaders(String[] headers, String... additionalHeaders) {
        assertEquals(headers.length, COMMON_COLUMNS.length + additionalHeaders.length);
        for (int i = 0; i < COMMON_COLUMNS.length; i++) {
            assertEquals(headers[i], COMMON_COLUMNS[i]);
        }
        for (int i = 0; i < additionalHeaders.length; i++) {
            assertEquals(headers[COMMON_COLUMNS.length + i], additionalHeaders[i]);
        }
    }

    private static void assertRow(String[] row, String... expectedValues) {
        assertEquals(row.length, expectedValues.length);
        for (int i = 0; i < expectedValues.length; i++) {
            assertEquals(row[i], expectedValues[i]);
        }
    }
}
