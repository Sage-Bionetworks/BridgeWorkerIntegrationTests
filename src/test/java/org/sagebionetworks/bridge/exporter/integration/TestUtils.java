package org.sagebionetworks.bridge.exporter.integration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.sqs.AmazonSQSClient;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.config.PropertiesConfig;
import org.sagebionetworks.bridge.rest.model.ClientInfo;
import org.sagebionetworks.bridge.sqs.SqsHelper;
import org.sagebionetworks.bridge.user.TestUserHelper;

public class TestUtils {
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

    public static SqsHelper getSqsHelper(AWSCredentialsProvider awsCredentialsProvider) {
        SqsHelper sqsHelper = new SqsHelper();
        //noinspection deprecation
        sqsHelper.setSqsClient(new AmazonSQSClient(awsCredentialsProvider));
        return sqsHelper;
    }
}
