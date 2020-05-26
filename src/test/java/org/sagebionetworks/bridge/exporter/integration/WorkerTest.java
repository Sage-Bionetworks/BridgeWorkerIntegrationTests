package org.sagebionetworks.bridge.exporter.integration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.table.QueryResultBundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.config.Environment;
import org.sagebionetworks.bridge.config.PropertiesConfig;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.api.HealthDataApi;
import org.sagebionetworks.bridge.rest.api.StudyReportsApi;
import org.sagebionetworks.bridge.rest.api.UploadSchemasApi;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.ClientInfo;
import org.sagebionetworks.bridge.rest.model.HealthDataRecord;
import org.sagebionetworks.bridge.rest.model.HealthDataSubmission;
import org.sagebionetworks.bridge.rest.model.ReportData;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.UploadFieldDefinition;
import org.sagebionetworks.bridge.rest.model.UploadFieldType;
import org.sagebionetworks.bridge.rest.model.UploadSchema;
import org.sagebionetworks.bridge.rest.model.UploadSchemaType;
import org.sagebionetworks.bridge.sqs.SqsHelper;
import org.sagebionetworks.bridge.user.TestUserHelper;

@SuppressWarnings("unchecked")
public class WorkerTest {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerTest.class);

    private static final String LARGE_TEXT_ATTACHMENT_FIELD_NAME = "my-large-text-attachment";
    private static final String LARGE_TEXT_ATTACHMENT_SCHEMA_ID = "large-text-attachment-test";
    private static final long LARGE_TEXT_ATTACHMENT_SCHEMA_REV = 1;
    private static final String PHONE_INFO = "BridgeWorkerIntegTest";
    private static final int POLL_INTERVAL_SECONDS = 5;
    private static final int POLL_MAX_ITERATIONS = 6;
    private static final String TEST_STUDY_ID = "api";

    private static final String CONFIG_FILE = "BridgeWorker-test.conf";
    private static final String DEFAULT_CONFIG_FILE = CONFIG_FILE;
    private static final String USER_CONFIG_FILE = System.getProperty("user.home") + "/" + CONFIG_FILE;

    // DailyActivitySummary.activities generally gets no data, and our integ test apps don't have permissions for
    // HeartRate.activities-heart-intraday.
    private static final Set<String> EXCLUDED_FITBIT_TABLE_SET = ImmutableSet.of("DailyActivitySummary.activities",
            "HeartRate.activities-heart-intraday");

    // services
    private static SqsHelper sqsHelper;
    private static SynapseClient synapseClient;

    private static String workerSqsUrl;

    private static DateTime now;

    // misc
    private static ExecutorService executorService;
    private static String integTestRunId;
    private static Table ddbFitBitTables;
    private static TestUserHelper.TestUser developer;
    private static TestUserHelper.TestUser user;

    // We want to only set up everything once for the entire test suite, not before each individual test. This means
    // using @BeforeClass, which unfortunately prevents us from using Spring.
    @BeforeClass
    public static void beforeClass() throws Exception {
        // Set TestUserHelper client info
        ClientInfo clientInfo = TestUserHelper.getClientInfo();
        clientInfo.setAppName("Worker Integ Tests");
        clientInfo.setAppVersion(1);

        // bridge config
        //noinspection ConstantConditions
        String defaultConfig = WorkerTest.class.getClassLoader().getResource(DEFAULT_CONFIG_FILE).getPath();
        Path defaultConfigPath = Paths.get(defaultConfig);
        Path localConfigPath = Paths.get(USER_CONFIG_FILE);

        Config bridgeConfig;
        if (Files.exists(localConfigPath)) {
            bridgeConfig = new PropertiesConfig(defaultConfigPath, localConfigPath);
        } else {
            bridgeConfig = new PropertiesConfig(defaultConfigPath);
        }

        // config vars
        Environment env = bridgeConfig.getEnvironment();
        workerSqsUrl = bridgeConfig.get("worker.request.sqs.queue.url");
        String synapseUser = bridgeConfig.get("synapse.user");
        String synapseApiKey = bridgeConfig.get("synapse.api.key");

        // AWS services
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(bridgeConfig.get("aws.key"),
                bridgeConfig.get("aws.secret.key"));
        AWSCredentialsProvider awsCredentialsProvider = new AWSStaticCredentialsProvider(awsCredentials);

        sqsHelper = new SqsHelper();
        //noinspection deprecation
        sqsHelper.setSqsClient(new AmazonSQSClient(awsCredentials));

        // DDB tables
        DynamoDB ddbClient = new DynamoDB(AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(awsCredentialsProvider).build());
        String ddbBridgePrefix = env.name().toLowerCase() + '-' + bridgeConfig.getUser() + '-';

        ddbFitBitTables = ddbClient.getTable(ddbBridgePrefix + "FitBitTables");
        Table ddbRecordTable = ddbClient.getTable(ddbBridgePrefix + "HealthDataRecord3");

        // Synapse clients
        synapseClient = new SynapseClientImpl();
        synapseClient.setUsername(synapseUser);
        synapseClient.setApiKey(synapseApiKey);

        // instantiate executor
        executorService = Executors.newCachedThreadPool();

        // Bridge clients
        developer = TestUserHelper.createAndSignInUser(WorkerTest.class, false, Role.DEVELOPER);
        user = TestUserHelper.createAndSignInUser(WorkerTest.class, true);

        // ensure schemas exist, so we have something to upload against
        UploadSchemasApi uploadSchemasApi = developer.getClient(UploadSchemasApi.class);

        // large-text-attachment-test schema
        UploadSchema largeTextAttachmentTestSchema = null;
        try {
            largeTextAttachmentTestSchema = uploadSchemasApi.getMostRecentUploadSchema(LARGE_TEXT_ATTACHMENT_SCHEMA_ID)
                    .execute().body();
        } catch (EntityNotFoundException ex) {
            // no-op
        }
        if (largeTextAttachmentTestSchema == null) {
            UploadFieldDefinition largeTextFieldDef = new UploadFieldDefinition()
                    .name(LARGE_TEXT_ATTACHMENT_FIELD_NAME).type(UploadFieldType.LARGE_TEXT_ATTACHMENT);
            largeTextAttachmentTestSchema = new UploadSchema().schemaId(LARGE_TEXT_ATTACHMENT_SCHEMA_ID)
                    .revision(LARGE_TEXT_ATTACHMENT_SCHEMA_REV).name("Large Text Attachment Test")
                    .schemaType(UploadSchemaType.IOS_DATA).addFieldDefinitionsItem(largeTextFieldDef);
            uploadSchemasApi.createUploadSchema(largeTextAttachmentTestSchema).execute();
        }

        // Take a snapshot of "now" so we don't get weird clock drift while the test is running.
        now = DateTime.now();

        // Clock skew on our Jenkins machine can be up to 10 minutes. Because of this, set the upload's upload time to
        // 20 min ago, and export in a window between 30 min ago and 10 min ago.
        DateTime uploadDateTime = now.minusMinutes(10);

        // Generate a test run ID
        integTestRunId = RandomStringUtils.randomAlphabetic(4);
        LOG.info("integTestRunId=" + integTestRunId);

        // Submit health data - Note that we build maps, since Jackson and GSON don't mix very well.
        Map<String, String> dataMap = new HashMap<>();
        dataMap.put(LARGE_TEXT_ATTACHMENT_FIELD_NAME, "This is my large text attachment");

        HealthDataSubmission submission = new HealthDataSubmission().appVersion("integTestRunId " + integTestRunId)
                .createdOn(uploadDateTime).phoneInfo(PHONE_INFO).schemaId(LARGE_TEXT_ATTACHMENT_SCHEMA_ID)
                .schemaRevision(LARGE_TEXT_ATTACHMENT_SCHEMA_REV).data(dataMap);

        HealthDataApi healthDataApi = user.getClient(HealthDataApi.class);
        HealthDataRecord record = healthDataApi.submitHealthData(submission).execute().body();
        String recordId = record.getId();
        LOG.info("Submitted health data with recordId " + recordId);

        // set uploadedOn to 10 min ago.
        ddbRecordTable.updateItem("id", recordId, new AttributeUpdate("uploadedOn")
                .put(uploadDateTime.getMillis()));
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (executorService != null) {
            executorService.shutdown();
        }

        if (developer != null) {
            developer.signOutAndDeleteUser();
        }

        if (user != null) {
            user.signOutAndDeleteUser();
        }
    }

    // Note: There is currently no way to automate the creation of users on FitBit.com. As a result, this test depends
    // on a pre-constructed user bridge-testing+fitbit@sagebase.org. The password is available in LastPass. The email
    // and password are the same for both Bridge and FitBit.
    //
    // This has already been configured for dev, uat, and prod. You may need to configure it on your own in the local
    // study.
    @Test
    public void fitbitWorker() throws Exception {
        LocalDate todaysDate = now.toLocalDate();

        // Poll Synapse tables and count how many rows.
        Map<String, Integer> oldCountsByTableId = countRowsForTables(todaysDate);

        // Create request.
        String requestText = "{\n" +
                "   \"service\":\"FitBitWorker\",\n" +
                "   \"body\":{\n" +
                "       \"date\":\"" + todaysDate.toString() + "\",\n" +
                "       \"studyWhitelist\":[\"" + TEST_STUDY_ID + "\"]\n" +
                "   }\n" +
                "}";
        ObjectNode requestNode = (ObjectNode) DefaultObjectMapper.INSTANCE.readTree(requestText);
        sqsHelper.sendMessageAsJson(workerSqsUrl, requestNode, 0);

        // We don't have a way of determining if the FitBit Worker is complete, and polling Synapse is expensive. Just
        // sleep for 30 seconds.
        TimeUnit.SECONDS.sleep(30);

        // Poll Synapse tables for new count and compare. Each table should have gone up.
        Map<String, Integer> newCountsByTableId = countRowsForTables(todaysDate);
        for (Map.Entry<String, Integer> oneNewCountEntry : newCountsByTableId.entrySet()) {
            String tableId = oneNewCountEntry.getKey();
            int newCount = oneNewCountEntry.getValue();

            // If the old table didn't exist before (eg, was newly created), the default value is 0 rows.
            assertTrue(newCount > oldCountsByTableId.getOrDefault(tableId, 0));
        }
    }

    private static Map<String, Integer> countRowsForTables(LocalDate createdDate)
            throws Exception {
        // Query dynamo for all FitBit tables in this study.
        Iterable<Item> tableItemIter = ddbFitBitTables.query("studyId", TEST_STUDY_ID);
        List<String> tableIdList = new ArrayList<>();
        for (Item oneTableItem : tableItemIter) {
            String tableName = oneTableItem.getString("tableId");
            if (!EXCLUDED_FITBIT_TABLE_SET.contains(tableName)) {
                tableIdList.add(oneTableItem.getString("synapseTableId"));
            }
        }

        // Kick off all table queries in parallel, to save on needless IO blocking.
        Map<String, Future<Integer>> futuresByTableId = new HashMap<>();
        for (String oneTableId : tableIdList) {
            Future<Integer> countFuture = executorService.submit(() -> countRows(oneTableId, createdDate));
            futuresByTableId.put(oneTableId, countFuture);
        }

        // Wait on all futures.
        Map<String, Integer> countsByTableId = new HashMap<>();
        for (Map.Entry<String, Future<Integer>> oneFutureEntry : futuresByTableId.entrySet()) {
            String tableId = oneFutureEntry.getKey();
            Future<Integer> countFuture = oneFutureEntry.getValue();
            int count = countFuture.get();
            countsByTableId.put(tableId, count);
        }

        return countsByTableId;
    }

    private static int countRows(String tableId, LocalDate createdDate) throws Exception {
        // Query Synapse
        String jobIdToken = synapseClient.queryTableEntityBundleAsyncStart("select * from " + tableId +
                        " where createdDate='" + createdDate.toString() + "'", 0L, null, true,
                SynapseClient.COUNT_PARTMASK, tableId);

        QueryResultBundle queryResultBundle = null;
        for (int i = 0; i < POLL_MAX_ITERATIONS; i++) {
            try {
                LOG.info("Retry get synapse table query result times: " + i);
                queryResultBundle = synapseClient.queryTableEntityBundleAsyncGet(jobIdToken, tableId);
                break;
            } catch (SynapseResultNotReadyException e) {
                TimeUnit.SECONDS.sleep(POLL_INTERVAL_SECONDS);
            }
        }
        assertNotNull(queryResultBundle);

        // count rows
        return queryResultBundle.getQueryCount().intValue();
    }

    @Test
    public void reporter() throws Exception {
        // Create request
        // Use signups report, since it's easier to create signups than uploads.
        // Start and end times should be 10 min before/after "now", to account for clock skew.
        // Even though the signups report is a "daily" report, we can specify arbitrary start and end times.
        DateTime startDateTime = now.minusMinutes(10);
        DateTime endDateTime = now.plusMinutes(10);
        String requestText = "{\n" +
                "   \"service\":\"REPORTER\",\n" +
                "   \"body\":{\n" +
                "       \"scheduler\":\"reporter-test-" + integTestRunId + "\",\n" +
                "       \"scheduleType\":\"DAILY_SIGNUPS\",\n" +
                "       \"studyWhitelist\":[\"" + TEST_STUDY_ID + "\"],\n" +
                "       \"startDateTime\":\"" + startDateTime.toString() + "\",\n" +
                "       \"endDateTime\":\"" + endDateTime.toString() + "\"\n" +
                "   }\n" +
                "}";
        ObjectNode requestNode = (ObjectNode) DefaultObjectMapper.INSTANCE.readTree(requestText);
        sqsHelper.sendMessageAsJson(workerSqsUrl, requestNode, 0);

        // Verify. Poll report until we get the result or we hit max iterations.
        StudyReportsApi reportsApi = developer.getClient(StudyReportsApi.class);
        String reportId = "reporter-test-" + integTestRunId + "-daily-signups-report";
        LocalDate reportDate = startDateTime.toLocalDate();
        List<ReportData> reportDataList = null;
        for (int i = 0; i < POLL_MAX_ITERATIONS; i++) {
            TimeUnit.SECONDS.sleep(POLL_INTERVAL_SECONDS);

            reportDataList = reportsApi.getStudyReportRecords(reportId, reportDate, reportDate).execute().body()
                    .getItems();
            if (!reportDataList.isEmpty()) {
                break;
            }
        }
        assertNotNull(reportDataList);
        assertFalse(reportDataList.isEmpty());

        // We should have at least one report with at least 2 users.
        assertEquals(reportDataList.size(), 1);
        ReportData reportData = reportDataList.get(0);
        assertEquals(reportData.getLocalDate(), reportDate);

        // For whatever reason, Bridge is returning this as a Double rather than an Int. To avoid dealing with double
        // rounding errors, we expect at least 2 users, but we'll check for > 1.9.
        Map<String, Map<String, Double>> reportMap = (Map<String, Map<String, Double>>)reportData.getData();
        Map<String, Double> reportByStatus = reportMap.get("byStatus");
        assertTrue(reportByStatus.get("enabled") > 1.9);
    }
}
