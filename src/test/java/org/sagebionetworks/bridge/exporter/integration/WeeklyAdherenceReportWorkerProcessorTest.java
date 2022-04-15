package org.sagebionetworks.bridge.exporter.integration;

import static org.sagebionetworks.bridge.rest.model.PerformanceOrder.SEQUENTIAL;
import static org.sagebionetworks.bridge.util.IntegTestUtils.SAGE_ID;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_ID;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.IOException;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.api.AssessmentsApi;
import org.sagebionetworks.bridge.rest.api.ParticipantsApi;
import org.sagebionetworks.bridge.rest.api.SchedulesV2Api;
import org.sagebionetworks.bridge.rest.api.StudyAdherenceApi;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.AdherenceReportSearch;
import org.sagebionetworks.bridge.rest.model.Assessment;
import org.sagebionetworks.bridge.rest.model.AssessmentReference2;
import org.sagebionetworks.bridge.rest.model.Schedule2;
import org.sagebionetworks.bridge.rest.model.Session;
import org.sagebionetworks.bridge.rest.model.TestFilter;
import org.sagebionetworks.bridge.rest.model.TimeWindow;
import org.sagebionetworks.bridge.rest.model.WeeklyAdherenceReport;
import org.sagebionetworks.bridge.rest.model.WeeklyAdherenceReportList;
import org.sagebionetworks.bridge.sqs.SqsHelper;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;

public class WeeklyAdherenceReportWorkerProcessorTest {
    
    private static final String STUDY_ID_1 = "study1";

    private SqsHelper sqsHelper;
    private String workerSqsUrl;
    private TestUser admin;
    private TestUser user;
    private String userId;
    
    private Schedule2 schedule;
    private Assessment assessment;
    
    SchedulesV2Api scheduleApi;
    AssessmentsApi assessmentApi;
    StudyAdherenceApi adherenceApi;
    ParticipantsApi participantApi;
    
    @BeforeMethod
    public void beforeMethod() throws Exception {
        Config bridgeConfig = TestUtils.loadConfig();
        workerSqsUrl = bridgeConfig.get("worker.request.sqs.queue.url");

        AWSCredentialsProvider awsCredentialsProvider = TestUtils.getAwsCredentialsForConfig(bridgeConfig);
        sqsHelper = TestUtils.getSqsHelper(awsCredentialsProvider);
        
        user = TestUserHelper.createAndSignInUser(getClass(), true);
        userId = user.getUserId();
        
        admin = TestUserHelper.getSignedInAdmin();
        scheduleApi = admin.getClient(SchedulesV2Api.class);
        adherenceApi = admin.getClient(StudyAdherenceApi.class);
        assessmentApi = admin.getClient(AssessmentsApi.class);
        participantApi = admin.getClient(ParticipantsApi.class);
        
        // We need to create a schedule in study1 for this user if it doesn't exist.
        try {
            scheduleApi.getScheduleForStudy(STUDY_ID_1).execute().body();
        } catch(EntityNotFoundException e) {
            String assessmentId = RandomStringUtils.randomAlphabetic(10);
            
            assessment = new Assessment()
                    .title(getClass().getSimpleName() + " " + assessmentId)
                    .osName("Universal")
                    .ownerId(SAGE_ID)
                    .identifier(assessmentId);
            assessment = assessmentApi.createAssessment(assessment).execute().body();
            
            AssessmentReference2 ref = new AssessmentReference2()
                    .guid(assessment.getGuid())
                    .appId(TEST_APP_ID)
                    .revision(5)
                    .title("A title")
                    .identifier(assessment.getIdentifier());
            
            Session session = new Session();
            session.setName("Simple session");
            session.addStartEventIdsItem("created_on");
            session.setPerformanceOrder(SEQUENTIAL);
            session.addAssessmentsItem(ref);
            session.addTimeWindowsItem(new TimeWindow().startTime("08:00").expiration("P3D"));

            schedule = new Schedule2();
            schedule.setName("Test Schedule [WeeklyAdherenceReportWorkerProcessorTest]");
            schedule.setDuration("P1W");
            schedule.addSessionsItem(session);

            schedule = scheduleApi.saveScheduleForStudy(STUDY_ID_1, schedule).execute().body();
        }
    }
    
    @AfterMethod
    public void afterMethod() throws IOException {
        if (schedule != null) {
            scheduleApi.deleteSchedule(schedule.getGuid()).execute().body();
            assessmentApi.deleteAssessment(assessment.getGuid(), true).execute().body();
        }
        if (user != null) {
            user.signOutAndDeleteUser();
        }
    }

    @Test
    public void requestCaching() throws Exception {
        String requestText = "{\"service\":\"WeeklyAdherenceReportWorker\", \"body\":{\"selectedStudies\":{\"api\":[\"study1\"]}}}";
        ObjectNode requestNode = (ObjectNode) DefaultObjectMapper.INSTANCE.readTree(requestText);

        sqsHelper.sendMessageAsJson(workerSqsUrl, requestNode, 0);
        
        // Wait. Let the worker do its thing.
        Thread.sleep(8000L);
        
        // This should return our user...
        assertTrue( reportCreatedForUser() );
        
        // This should cascade delete the user's report
        user.signOutAndDeleteUser();
        user = null;
        
        assertFalse( reportCreatedForUser() );
    }
    
    private boolean reportCreatedForUser() throws Exception {
        int offset = 0;
        WeeklyAdherenceReportList list = null;
        while (list == null || !list.getItems().isEmpty()) {
            AdherenceReportSearch search = new AdherenceReportSearch()
                    .testFilter(TestFilter.TEST).pageSize(100).offsetBy(offset);
            list = adherenceApi.getWeeklyAdherenceReports(
                    STUDY_ID_1, search).execute().body();
            for (WeeklyAdherenceReport report : list.getItems()) {
                if (report.getParticipant().getIdentifier().equals(userId)) {
                    return true;
                }
            }
            offset += 100;
        }
        return false;
    }

}
