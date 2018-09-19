package org.sagebionetworks.bridge.exporter.integration;

import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PrivateKey;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.bouncycastle.cms.CMSException;
import org.joda.time.DateTime;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.config.PropertiesConfig;
import org.sagebionetworks.bridge.crypto.BcCmsEncryptor;
import org.sagebionetworks.bridge.crypto.CmsEncryptor;
import org.sagebionetworks.bridge.crypto.PemUtils;
import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.UploadSchemasApi;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.ClientInfo;
import org.sagebionetworks.bridge.rest.model.UploadFieldDefinition;
import org.sagebionetworks.bridge.rest.model.UploadFieldType;
import org.sagebionetworks.bridge.rest.model.UploadRequest;
import org.sagebionetworks.bridge.rest.model.UploadSchema;
import org.sagebionetworks.bridge.rest.model.UploadSchemaType;
import org.sagebionetworks.bridge.rest.model.UploadSession;
import org.sagebionetworks.bridge.rest.model.UploadValidationStatus;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.SqsHelper;
import org.sagebionetworks.bridge.user.TestUserHelper;

public class TestUtils {
    public static final String LARGE_TEXT_ATTACHMENT_FIELD_NAME = "my-large-text-attachment";
    public static final String LARGE_TEXT_ATTACHMENT_SCHEMA_ID = "large-text-attachment-test";
    public static final long LARGE_TEXT_ATTACHMENT_SCHEMA_REV = 1;
    private static final int MAX_POLL_ITERATIONS = 6;
    private static final long POLL_DELAY_MILLIS = 5000;

    private static final String CONFIG_FILE = "BridgeWorker-test.conf";
    private static final String DEFAULT_CONFIG_FILE = CONFIG_FILE;
    private static final String USER_CONFIG_FILE = System.getProperty("user.home") + "/" + CONFIG_FILE;

    public static Config loadConfig() throws IOException {
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

        return bridgeConfig;
    }

    public static AWSCredentialsProvider getAwsCredentialsForConfig(Config bridgeConfig) {
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(bridgeConfig.get("aws.key"),
                bridgeConfig.get("aws.secret.key"));
        return new AWSStaticCredentialsProvider(awsCredentials);
    }

    public static DynamoDB getDdbClient(AWSCredentialsProvider awsCredentialsProvider) {
        return new DynamoDB(AmazonDynamoDBClientBuilder.standard().withCredentials(awsCredentialsProvider).build());
    }

    public static Table getDdbTable(Config bridgeConfig, DynamoDB ddbClient, String shortName) {
        return ddbClient.getTable(bridgeConfig.getEnvironment().name().toLowerCase() + '-' +
                bridgeConfig.getUser() + '-' + shortName);
    }

    public static void ensureSchemas(TestUserHelper.TestUser developer) throws IOException {
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
    }

    public static SqsHelper getSqsHelper(AWSCredentialsProvider awsCredentialsProvider) {
        SqsHelper sqsHelper = new SqsHelper();
        //noinspection deprecation
        sqsHelper.setSqsClient(new AmazonSQSClient(awsCredentialsProvider));
        return sqsHelper;
    }

    public static UploadValidationStatus upload(Config config, S3Helper s3Helper, TestUserHelper.TestUser user)
            throws CertificateEncodingException, CMSException, IOException {
        // Create file contents.
        String infoJsonText = "{\n" +
                "   \"appVersion\":\"version 1.0.0, build 1\",\n" +
                "   \"createdOn\":\"" + DateTime.now() + "\",\n" +
                "   \"format\":\"v2_generic\",\n" +
                "   \"item\":\"" + LARGE_TEXT_ATTACHMENT_SCHEMA_ID + "\",\n" +
                "   \"phoneInfo\":\"Worker Integ Tests\",\n" +
                "   \"schemaRevision\":" + LARGE_TEXT_ATTACHMENT_SCHEMA_REV + "\n" +
                "}";

        String attachmentText = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Phasellus orci aliquam.";

        Map<String, String> fileMap = ImmutableMap.<String, String>builder()
                .put("info.json", infoJsonText)
                .put(LARGE_TEXT_ATTACHMENT_FIELD_NAME, attachmentText)
                .build();

        // Zip.
        byte[] zippedBytes;
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final ZipOutputStream zos = new ZipOutputStream(baos)) {
            for (Map.Entry<String, String> oneData : fileMap.entrySet()) {
                ZipEntry zipEntry = new ZipEntry(oneData.getKey());
                zos.putNextEntry(zipEntry);
                zos.write(oneData.getValue().getBytes());
                zos.closeEntry();
            }
            zos.flush();
            zippedBytes = baos.toByteArray();
        }

        // Encrypt. For whatever reason, the encryptor requires both the public and the private key.
        // TODO: Fix the encryptor to require only one of the keys, so that the integ tests don't need to download the
        // private key.
        String certPem = s3Helper.readS3FileAsString(config.get("upload.cms.cert.bucket"), "api.pem");
        X509Certificate cert = PemUtils.loadCertificateFromPem(certPem);

        // download private key
        String privKeyPem = s3Helper.readS3FileAsString(config.get("upload.cms.priv.bucket"), "api.pem");
        PrivateKey privKey = PemUtils.loadPrivateKeyFromPem(privKeyPem);

        CmsEncryptor encryptor = new BcCmsEncryptor(cert, privKey);
        byte[] encryptedBytes = encryptor.encrypt(zippedBytes);

        // Write to temp file.
        File tempDir = com.google.common.io.Files.createTempDir();
        File encryptedFile = new File(tempDir, "upload-encrypted");
        com.google.common.io.Files.write(encryptedBytes, encryptedFile);

        // Upload.
        ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);
        UploadRequest request = RestUtils.makeUploadRequestForFile(encryptedFile);
        UploadSession session = usersApi.requestUploadSession(request).execute().body();
        RestUtils.uploadToS3(encryptedFile, session.getUrl());
        return usersApi.completeUploadSession(session.getId(), true).execute().body();
    }

    public static long getWorkerLastFinishedTime(Table workerLogTable, String workerId) {
        // To get the latest worker time, sort the index in reverse and limit the result set to 1.
        QuerySpec query = new QuerySpec()
                .withHashKey("workerId", workerId)
                .withScanIndexForward(false).withMaxResultSize(1);
        Iterator<Item> itemIter = workerLogTable.query(query).iterator();
        if (itemIter.hasNext()) {
            Item item = itemIter.next();
            return item.getLong("finishTime");
        } else {
            // Arbitrarily return 0. That's far enough in the past that any reasonable result will be after this.
            return 0;
        }
    }

    // Polls the worker log until the worker is finished, as determined by a new timestamp after the one specified.
    public static void pollWorkerLog(Table workerLogTable, String workerId, long previousFinishTime) throws Exception {
        long finishTime = previousFinishTime;
        for (int i = 0; i < MAX_POLL_ITERATIONS; i++) {
            Thread.sleep(POLL_DELAY_MILLIS);
            finishTime = getWorkerLastFinishedTime(workerLogTable, workerId);
            if (finishTime > previousFinishTime) {
                break;
            }
        }
        assertTrue(finishTime > previousFinishTime, "Worker log has updated finish time");
    }
}
