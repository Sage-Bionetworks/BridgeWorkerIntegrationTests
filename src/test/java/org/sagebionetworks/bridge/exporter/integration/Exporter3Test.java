package org.sagebionetworks.bridge.exporter.integration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.sagebionetworks.client.SynapseAdminClientImpl;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseStsCredentialsProvider;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.EntityChildrenRequest;
import org.sagebionetworks.repo.model.EntityChildrenResponse;
import org.sagebionetworks.repo.model.EntityHeader;
import org.sagebionetworks.repo.model.EntityType;
import org.sagebionetworks.repo.model.annotation.v2.Annotations;
import org.sagebionetworks.repo.model.annotation.v2.AnnotationsValue;
import org.sagebionetworks.repo.model.annotation.v2.AnnotationsValueType;
import org.sagebionetworks.repo.model.sts.StsPermission;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.crypto.BcCmsEncryptor;
import org.sagebionetworks.bridge.crypto.PemUtils;
import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.api.ForAdminsApi;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForDevelopersApi;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.Exporter3Configuration;
import org.sagebionetworks.bridge.rest.model.HealthDataRecordEx3;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.UploadRequest;
import org.sagebionetworks.bridge.rest.model.UploadSession;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.user.TestUserHelper;
import org.sagebionetworks.bridge.util.IntegTestUtils;

@SuppressWarnings({ "ConstantConditions", "deprecation", "OptionalGetWithoutIsPresent" })
public class Exporter3Test {
    private static final String CONFIG_KEY_RAW_DATA_BUCKET = "health.data.bucket.raw";
    private static final String CONFIG_KEY_SYNAPSE_API_KEY = "synapse.api.key";
    private static final String CONFIG_KEY_SYNAPSE_USER = "synapse.user";
    private static final String CONTENT_TYPE_TEXT_PLAIN = "text/plain";
    private static final String CUSTOM_METADATA_KEY = "custom-metadata-key";
    private static final String CUSTOM_METADATA_KEY_SANITIZED = "custom_metadata_key";
    private static final String CUSTOM_METADATA_VALUE = "custom-metadata-value";
    private static final byte[] UPLOAD_CONTENT = "This is the upload content".getBytes(StandardCharsets.UTF_8);

    private static TestUserHelper.TestUser adminDeveloperWorker;
    private static Exporter3Configuration ex3Config;
    private static String rawDataBucket;
    private static SynapseClient synapseClient;
    private static TestUserHelper.TestUser user;

    @BeforeClass
    public static void beforeClass() throws Exception {
        Config config = TestUtils.loadConfig();
        rawDataBucket = config.get(CONFIG_KEY_RAW_DATA_BUCKET);

        // Set up SynapseClient.
        String synapseUserName = config.get(CONFIG_KEY_SYNAPSE_USER);
        String synapseApiKey = config.get(CONFIG_KEY_SYNAPSE_API_KEY);

        synapseClient = new SynapseAdminClientImpl();
        synapseClient.setUsername(synapseUserName);
        synapseClient.setApiKey(synapseApiKey);

        // Create admin account.
        adminDeveloperWorker = TestUserHelper.createAndSignInUser(Exporter3Test.class, false, Role.ADMIN,
                Role.DEVELOPER, Role.WORKER);

        // Wipe the Exporter 3 Config and re-create it.
        ForAdminsApi adminsApi = adminDeveloperWorker.getClient(ForAdminsApi.class);

        // Reset the Exporter 3 Config.
        App app = adminsApi.getUsersApp().execute().body();
        app.setExporter3Configuration(null);
        app.setExporter3Enabled(false);
        adminsApi.updateUsersApp(app).execute();

        // Init Exporter 3.
        ex3Config = adminsApi.initExporter3().execute().body();

        // Create user.
        user = TestUserHelper.createAndSignInUser(Exporter3Test.class, true);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (user != null) {
            user.signOutAndDeleteUser();
        }
        if (adminDeveloperWorker != null) {
            adminDeveloperWorker.signOutAndDeleteUser();
        }
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

    private void testUpload(byte[] content, boolean encrypted) throws Exception {
        // Create a temp file so that we can use RestUtils.
        File file = File.createTempFile("text", ".txt");
        String filename = file.getName();
        Files.write(content, file);

        // Create upload request. We want to add custom metadata. Also, RestUtils defaults to application/zip. We want
        // to overwrite this.
        UploadRequest uploadRequest = RestUtils.makeUploadRequestForFile(file);
        uploadRequest.setContentType(CONTENT_TYPE_TEXT_PLAIN);
        uploadRequest.putMetadataItem(CUSTOM_METADATA_KEY, CUSTOM_METADATA_VALUE);
        uploadRequest.setEncrypted(encrypted);
        uploadRequest.setZipped(false);

        // Upload.
        ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);
        UploadSession session = usersApi.requestUploadSession(uploadRequest).execute().body();
        String uploadId = session.getId();
        RestUtils.uploadToS3(file, session.getUrl(), CONTENT_TYPE_TEXT_PLAIN);
        usersApi.completeUploadSession(session.getId(), true, false).execute();

        // Verify Synapse file entity and annotations.
        String rawFolderId = ex3Config.getRawDataFolderId();
        String todaysDateString = LocalDate.now(TestUtils.LOCAL_TIME_ZONE).toString();
        String exportedFilename = uploadId + '-' + filename;

        // First, get the exported file.
        String todayFolderId = getSynapseChildByName(rawFolderId, todaysDateString, EntityType.folder);
        String exportedFileId = getSynapseChildByName(todayFolderId, exportedFilename, EntityType.file);

        // Now verify the annotations.
        Annotations annotations = synapseClient.getAnnotationsV2(exportedFileId);
        Map<String, AnnotationsValue> annotationMap = annotations.getAnnotations();
        Map<String, String> flattenedAnnotationMap = new HashMap<>();
        for (Map.Entry<String, AnnotationsValue> annotationEntry : annotationMap.entrySet()) {
            String annotationKey = annotationEntry.getKey();
            AnnotationsValue annotationsValue = annotationEntry.getValue();
            assertEquals(annotationsValue.getType(), AnnotationsValueType.STRING);
            assertEquals(annotationsValue.getValue().size(), 1);
            flattenedAnnotationMap.put(annotationKey, annotationsValue.getValue().get(0));
        }
        verifyMetadata(flattenedAnnotationMap, uploadId);

        // Get the STS token and verify the file in S3.
        AWSCredentialsProvider awsCredentialsProvider = new SynapseStsCredentialsProvider(synapseClient, rawFolderId,
                StsPermission.read_only);
        AmazonS3Client s3Client = new AmazonS3Client(awsCredentialsProvider).withRegion(Regions.US_EAST_1);
        S3Helper s3Helper = new S3Helper();
        s3Helper.setS3Client(s3Client);

        String expectedS3Key = IntegTestUtils.TEST_APP_ID + '/' + todaysDateString + '/' + exportedFilename;
        byte[] s3Bytes = s3Helper.readS3FileAsBytes(rawDataBucket, expectedS3Key);
        assertEquals(s3Bytes, UPLOAD_CONTENT);

        ObjectMetadata s3Metadata = s3Helper.getObjectMetadata(rawDataBucket, expectedS3Key);
        assertEquals(s3Metadata.getSSEAlgorithm(), ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        assertEquals(s3Metadata.getContentType(), CONTENT_TYPE_TEXT_PLAIN);
        verifyMetadata(s3Metadata.getUserMetadata(), uploadId);

        // Verify the record in Bridge.
        ForWorkersApi workersApi = adminDeveloperWorker.getClient(ForWorkersApi.class);
        HealthDataRecordEx3 record = workersApi.getRecordEx3(IntegTestUtils.TEST_APP_ID, uploadId).execute().body();
        assertTrue(record.isExported());

        // Timestamps are relatively recent. Because of clock skew on Jenkins, give a very generous time window of,
        // let's say, 1 hour.
        DateTime oneHourAgo = DateTime.now().minusHours(1);
        assertTrue(record.getExportedOn().isAfter(oneHourAgo));
    }

    private String getSynapseChildByName(String parentId, String childName, EntityType type) throws SynapseException {
        // This only works because we clean up the Synapse project at the start of every test run, so there are only
        // 1-2 children.
        EntityChildrenRequest request = new EntityChildrenRequest();
        request.setParentId(parentId);
        request.setIncludeTypes(ImmutableList.of(type));
        EntityChildrenResponse response = synapseClient.getEntityChildren(request);
        return response.getPage().stream().filter(h -> childName.equals(h.getName())).map(EntityHeader::getId)
                .findAny().get();
    }

    private static void verifyMetadata(Map<String, String> metadataMap, String expectedRecordId) {
        assertEquals(metadataMap.size(), 6);
        assertTrue(metadataMap.containsKey("clientInfo"));
        assertTrue(metadataMap.containsKey("healthCode"));
        assertEquals(metadataMap.get("recordId"), expectedRecordId);
        assertEquals(metadataMap.get(CUSTOM_METADATA_KEY_SANITIZED), CUSTOM_METADATA_VALUE);

        // Timestamps are relatively recent. Because of clock skew on Jenkins, give a very generous time window of,
        // let's say, 1 hour.
        DateTime oneHourAgo = DateTime.now().minusHours(1);
        DateTime exportedOn = DateTime.parse(metadataMap.get("exportedOn"));
        assertTrue(exportedOn.isAfter(oneHourAgo));

        DateTime uploadedOn = DateTime.parse(metadataMap.get("uploadedOn"));
        assertTrue(uploadedOn.isAfter(oneHourAgo));
    }
}
