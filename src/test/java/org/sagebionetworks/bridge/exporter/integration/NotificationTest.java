package org.sagebionetworks.bridge.exporter.integration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.api.ActivitiesApi;
import org.sagebionetworks.bridge.rest.api.ParticipantsApi;
import org.sagebionetworks.bridge.rest.api.SchedulesApi;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.model.Activity;
import org.sagebionetworks.bridge.rest.model.IdentifierUpdate;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.Schedule;
import org.sagebionetworks.bridge.rest.model.SchedulePlan;
import org.sagebionetworks.bridge.rest.model.ScheduleStrategy;
import org.sagebionetworks.bridge.rest.model.ScheduleType;
import org.sagebionetworks.bridge.rest.model.ScheduledActivity;
import org.sagebionetworks.bridge.rest.model.SignUp;
import org.sagebionetworks.bridge.rest.model.SimpleScheduleStrategy;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.TaskReference;
import org.sagebionetworks.bridge.sqs.SqsHelper;
import org.sagebionetworks.bridge.user.TestUserHelper;
import org.sagebionetworks.bridge.util.IntegTestUtils;

@SuppressWarnings("ConstantConditions")
public class NotificationTest {
    private static final String EXCLUDED_DATA_GROUP = "integ-test-excluded";
    private static final DateTimeZone LOCAL_TIME_ZONE = DateTimeZone.forID("America/Los_Angeles");
    private static final int MAX_POLL_ITERATIONS = 6;
    private static final String MESSAGE_CUMULATIVE = "Test notification (cumulative activities)";
    private static final String MESSAGE_EARLY_1 = "Test notification (early group 1)";
    private static final String MESSAGE_EARLY_2 = "Test notification (early group 2)";
    private static final String MESSAGE_LATE = "Test notification (late in burst)";
    private static final String MESSAGE_PRE_BURST = "Test notification (pre-burst)";
    private static final long POLL_DELAY_MILLIS = 5000;
    private static final String REQUIRED_DATA_GROUP_1 = "sdk-int-1";
    private static final String REQUIRED_DATA_GROUP_2 = "sdk-int-2";

    // Use this unique ID for event IDs, schedule labels, task IDs, etc.
    private static final String TEST_ID = "notification-integ-test";

    private static LocalDate defaultTestDate;
    private static LocalDate today;
    private static SqsHelper sqsHelper;
    private static String workerSqsUrl;
    private static Table ddbNotificationLogTable;
    private static Table ddbWorkerLogTable;
    private static TestUserHelper.TestUser developer;
    private static TestUserHelper.TestUser researcher;

    private TestUserHelper.TestUser user;

    // We want to only set up everything once for the entire test suite, not before each individual test. This means
    // using @BeforeClass, which unfortunately prevents us from using Spring.
    @BeforeClass
    public static void beforeClass() throws Exception {
        // AWS
        Config bridgeConfig = TestUtils.loadConfig();
        AWSCredentialsProvider awsCredentialsProvider = TestUtils.getAwsCredentialsForConfig(bridgeConfig);

        // DDB tables
        DynamoDB ddbClient = TestUtils.getDdbClient(awsCredentialsProvider);
        Table ddbNotificationConfigTable = TestUtils.getDdbTable(bridgeConfig, ddbClient,
                "NotificationConfig");
        ddbNotificationLogTable = TestUtils.getDdbTable(bridgeConfig, ddbClient, "NotificationLog");
        ddbWorkerLogTable = TestUtils.getDdbTable(bridgeConfig, ddbClient, "WorkerLog");

        // Ensure notification config
        Map<String, String> missedCumulativeMessageMap = ImmutableMap.of(
                REQUIRED_DATA_GROUP_1, MESSAGE_CUMULATIVE,
                REQUIRED_DATA_GROUP_2, MESSAGE_CUMULATIVE);
        Map<String, String> missedEarlyMessageMap = ImmutableMap.of(
                REQUIRED_DATA_GROUP_1, MESSAGE_EARLY_1,
                REQUIRED_DATA_GROUP_2, MESSAGE_EARLY_2);
        Map<String, String> missedLateMessageMap = ImmutableMap.of(
                REQUIRED_DATA_GROUP_1, MESSAGE_LATE,
                REQUIRED_DATA_GROUP_2, MESSAGE_LATE);
        Map<String, String> preburstMessageMap = ImmutableMap.of(
                REQUIRED_DATA_GROUP_1, MESSAGE_PRE_BURST,
                REQUIRED_DATA_GROUP_2, MESSAGE_PRE_BURST);

        Item configItem = new Item().withPrimaryKey("studyId", IntegTestUtils.STUDY_ID)
                .withInt("burstDurationDays", 9)
                .withStringSet("burstStartEventIdSet", "enrollment", "custom:" + TEST_ID)
                .withString("burstTaskId", TEST_ID)
                .withInt("earlyLateCutoffDays", 5)
                .withStringSet("excludedDataGroupSet", EXCLUDED_DATA_GROUP)
                .withMap("missedCumulativeActivitiesMessagesByDataGroup", missedCumulativeMessageMap)
                .withMap("missedEarlyActivitiesMessagesByDataGroup", missedEarlyMessageMap)
                .withMap("missedLaterActivitiesMessagesByDataGroup", missedLateMessageMap)
                .withInt("notificationBlackoutDaysFromStart", 3)
                .withInt("notificationBlackoutDaysFromEnd", 3)
                .withInt("numMissedConsecutiveDaysToNotify", 2)
                .withInt("numMissedDaysToNotify", 3)
                .withMap("preburstMessagesByDataGroup", preburstMessageMap)
                .withStringSet("requiredDataGroupsOneOfSet", REQUIRED_DATA_GROUP_1, REQUIRED_DATA_GROUP_2)
                .withStringSet("requiredSubpopulationGuidSet", IntegTestUtils.STUDY_ID);
        ddbNotificationConfigTable.putItem(configItem);

        // SQS
        workerSqsUrl = bridgeConfig.get("worker.request.sqs.queue.url");
        sqsHelper = TestUtils.getSqsHelper(awsCredentialsProvider);

        // Create Bridge accounts
        developer = TestUserHelper.createAndSignInUser(NotificationTest.class, false, Role.DEVELOPER);
        researcher = TestUserHelper.createAndSignInUser(NotificationTest.class, false, Role.RESEARCHER);

        // Ensure study has all the pre-reqs for our test.
        Study study = developer.getClient(StudiesApi.class).getUsersStudy().execute().body();
        boolean shouldUpdateStudy = false;

        if (!study.getAutomaticCustomEvents().containsKey(TEST_ID)) {
            study.putAutomaticCustomEventsItem(TEST_ID, "P2W");
            shouldUpdateStudy = true;
        }

        if (!study.getDataGroups().contains(EXCLUDED_DATA_GROUP)) {
            study.addDataGroupsItem(EXCLUDED_DATA_GROUP);
            shouldUpdateStudy = true;
        }
        if (!study.getDataGroups().contains(REQUIRED_DATA_GROUP_1)) {
            study.addDataGroupsItem(REQUIRED_DATA_GROUP_1);
            shouldUpdateStudy = true;
        }
        if (!study.getDataGroups().contains(REQUIRED_DATA_GROUP_2)) {
            study.addDataGroupsItem(REQUIRED_DATA_GROUP_2);
            shouldUpdateStudy = true;
        }

        if (!study.getTaskIdentifiers().contains(TEST_ID)) {
            study.addTaskIdentifiersItem(TEST_ID);
            shouldUpdateStudy = true;
        }

        if (shouldUpdateStudy) {
            developer.getClient(StudiesApi.class).updateUsersStudy(study).execute();
        }

        // Make sure we have a schedule for our integ test.
        List<SchedulePlan> schedulePlanList = developer.getClient(SchedulesApi.class).getSchedulePlans().execute()
                .body().getItems();
        Optional<SchedulePlan> optionalSchedulePlan = schedulePlanList.stream()
                .filter(s -> TEST_ID.equals(s.getLabel())).findAny();
        if (!optionalSchedulePlan.isPresent()) {
            TaskReference task = new TaskReference().identifier(TEST_ID);
            Activity activity = new Activity().label(TEST_ID).task(task);
            Schedule schedule = new Schedule().label(TEST_ID).scheduleType(ScheduleType.RECURRING)
                    .eventId("enrollment,custom:" + TEST_ID).expires("P1D").interval("P1D").sequencePeriod("P9D")
                    .addTimesItem("00:00").addActivitiesItem(activity);
            ScheduleStrategy strategy = new SimpleScheduleStrategy().schedule(schedule).type("SimpleScheduleStrategy");
            SchedulePlan newPlan = new SchedulePlan().label(TEST_ID).strategy(strategy);
            developer.getClient(SchedulesApi.class).createSchedulePlan(newPlan).execute();
        }

        // Make snapshots of certain event times, so we don't have random clock skew errors.
        today = LocalDate.now(LOCAL_TIME_ZONE);

        // The default test date is 3 days after enrollment. This is within the first study burst after the blackout.
        defaultTestDate = today.plusDays(3);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (developer != null) {
            developer.signOutAndDeleteUser();
        }
        if (researcher != null) {
            researcher.signOutAndDeleteUser();
        }
    }

    @BeforeMethod
    public void before() throws Exception {
        // Ensure no user with the test phone number
        IntegTestUtils.deletePhoneUser(researcher);
    }

    @AfterMethod
    public void after() throws Exception {
        if (user != null) {
            user.signOutAndDeleteUser();
        }
    }

    @Test
    public void phoneNotVerified() throws Exception {
        // Make an email user, and then add a phone number. (Phone is unverified by default.)
        SignUp signUp = new SignUp().study(IntegTestUtils.STUDY_ID)
                .email(IntegTestUtils.makeEmail(NotificationTest.class)).password("password1");
        signUp.addDataGroupsItem(REQUIRED_DATA_GROUP_1);
        user = TestUserHelper.createAndSignInUser(NotificationTest.class, true, signUp);
        initUser(user);

        IdentifierUpdate identifierUpdate = new IdentifierUpdate().phoneUpdate(IntegTestUtils.PHONE)
                .signIn(user.getSignIn());
        user.getClient(ParticipantsApi.class).updateUsersIdentifiers(identifierUpdate);

        // Run test
        testNoNotification("phoneNotVerified", null, user);
    }

    @Test
    public void notConsented() throws Exception {
        // Make unconsented phone user. Note that unconsented users can't get activities.
        SignUp signUp = new SignUp().study(IntegTestUtils.STUDY_ID).phone(IntegTestUtils.PHONE).password("password1");
        signUp.addDataGroupsItem(REQUIRED_DATA_GROUP_1);
        user = TestUserHelper.createAndSignInUser(NotificationTest.class, false, signUp);

        // Run test
        testNoNotification("notConsent", null, user);
    }

    @Test
    public void noActivities() throws Exception {
        // Create a user but don't init their activities.
        user = createUser();

        // Run test
        testNoNotification("noActivities", null, user);
    }

    @Test
    public void missingRequiredDataGroup() throws Exception {
        // Create user that's missing the required data group
        SignUp signUp = new SignUp().study(IntegTestUtils.STUDY_ID).phone(IntegTestUtils.PHONE).password("password1");
        user = TestUserHelper.createAndSignInUser(NotificationTest.class, true, signUp);
        initUser(user);

        // Run test
        testNoNotification("missingRequiredDataGroup", null, user);
    }

    @Test
    public void excludedByDataGroup() throws Exception {
        // Create user with an excluded data group
        SignUp signUp = new SignUp().study(IntegTestUtils.STUDY_ID).phone(IntegTestUtils.PHONE).password("password1");
        signUp.addDataGroupsItem(REQUIRED_DATA_GROUP_1);
        signUp.addDataGroupsItem(EXCLUDED_DATA_GROUP);
        user = TestUserHelper.createAndSignInUser(NotificationTest.class, true, signUp);
        initUser(user);

        // Run test
        testNoNotification("excludeByDataGroup", null, user);
    }

    @Test
    public void timeZoneOutOfRange() throws Exception {
        // Create user and initialize with an offset that's definitely out of range (UTC).
        TestUserHelper.TestUser user = createUser();

        // Init user's activities using UTC
        DateTime startOfToday = today.toDateTimeAtStartOfDay(DateTimeZone.UTC);
        user.getClient(ActivitiesApi.class).getScheduledActivitiesByDateRange(startOfToday, startOfToday.plusDays(14))
                .execute();
        user.getClient(ActivitiesApi.class).getScheduledActivitiesByDateRange(startOfToday.plusDays(14),
                startOfToday.plusDays(28)).execute();

        // Run test
        testNoNotification("timeZoneOutOfRange", null, user);
    }

    @Test
    public void notInBurst() throws Exception {
        // Call Notification Worker on a date between the bursts. Bursts lasts 9 days. Today + 9 days is the day after
        // the end of the first burst.
        user = createAndInitUser();
        testNoNotification("betweenBursts", today.plusDays(9), user);
    }

    @Test
    public void blackoutHead() throws Exception {
        // On today + 2, the user will have missed 3 days in a row, but we're still in the blackout period.
        user = createAndInitUser();
        testNoNotification("blackoutHead", today.plusDays(2), user);
    }

    @Test
    public void blackoutTail() throws Exception {
        // On today + 6, the user will have missed 7 days in a row, but we just entered the tail blackout period.
        user = createAndInitUser();
        testNoNotification("blackoutTail", today.plusDays(6), user);
    }

    @Test
    public void didPreviousActivities() throws Exception {
        // Create user and do activities on first, second, and third days.
        user = createAndInitUser();
        completeActivitiesForDateIndices(user, 0, 1, 2);

        // Run test
        testNoNotification("didPreviousActivities", null, user);
    }

    @Test
    public void didTodaysActivities() throws Exception {
        // User missed 3 days in a row, but did today's activities, so we won't notify.
        user = createAndInitUser();
        completeActivitiesForDateIndices(user, 3);

        // Run test
        testNoNotification("didTodaysActivities", null, user);
    }

    private static void testNoNotification(String testName, LocalDate date, TestUserHelper.TestUser user)
            throws Exception {
        // Execute
        executeNotificationWorker(testName, date);

        // Verify no entries in the notification log.
        Iterator<Item> itemIter = ddbNotificationLogTable.query("userId", user.getUserId()).iterator();
        assertFalse(itemIter.hasNext());
    }

    @Test
    public void preburstAndEarlyNotifications() throws Exception {
        // Test preburst and early notifications in the same test, specifically to make sure that the preburst
        // notification doesn't prevent the regular notification from happening.
        user = createAndInitUser();

        // Technically, the notification worker will never process a user _before_ they're enrolled. But for the
        // purposes of this test, this represents sending the pre-burst notification a day before the start of burst.
        List<Item> notificationList = getNotificationsForUser("preburst", today.minusDays(1), user);
        assertEquals(notificationList.size(), 1);

        Item preburstNotification = notificationList.get(0);
        assertEquals(preburstNotification.getString("notificationType"), "PRE_BURST");
        assertEquals(preburstNotification.getString("message"), MESSAGE_PRE_BURST);

        // Did first day, but missed the second and third days.
        completeActivitiesForDateIndices(user, 0);

        // Run test for normal notification. We have 2 notifications now, and the first one should be the same as the
        // one we saw earlier.
        notificationList = getNotificationsForUser("twoConsecutiveDays", null, user);
        assertEquals(notificationList.size(), 2);

        assertEquals(notificationList.get(0), preburstNotification);

        assertEquals(notificationList.get(1).getString("notificationType"), "EARLY");
        assertEquals(notificationList.get(1).getString("message"), MESSAGE_EARLY_1);
    }

    @Test
    public void threeTotalDays() throws Exception {
        // Missed days 0, 2, and 4.
        user = createAndInitUser();
        completeActivitiesForDateIndices(user, 1, 3);

        // Run test. Test on day 4.
        List<Item> notificationList = getNotificationsForUser("threeTotalDays", today.plusDays(4), user);
        assertEquals(notificationList.size(), 1);
        assertEquals(notificationList.get(0).getString("notificationType"), "CUMULATIVE");
        assertEquals(notificationList.get(0).getString("message"), MESSAGE_CUMULATIVE);
    }

    @Test
    public void missedLateActivities() throws Exception {
        // Did activities on day 0-3, but missed days 4 and 5. Run test on day 5.
        // Did first day, but missed the second and third days.
        user = createAndInitUser();
        completeActivitiesForDateIndices(user, 0, 1, 2, 3);

        // Run test
        List<Item> notificationList = getNotificationsForUser("missedLateActivities", today.plusDays(5),
                user);
        assertEquals(notificationList.size(), 1);
        assertEquals(notificationList.get(0).getString("notificationType"), "LATE");
        assertEquals(notificationList.get(0).getString("message"), MESSAGE_LATE);
    }

    @Test
    public void differentMessageByDataGroup() throws Exception {
        // Create user with data group 2.
        SignUp signUp = new SignUp().study(IntegTestUtils.STUDY_ID).phone(IntegTestUtils.PHONE).password("password1");
        signUp.addDataGroupsItem(REQUIRED_DATA_GROUP_2);
        user = TestUserHelper.createAndSignInUser(NotificationTest.class, true, signUp);
        initUser(user);

        // Did first day, but missed the second and third days.
        completeActivitiesForDateIndices(user, 0);

        // Run test
        List<Item> notificationList = getNotificationsForUser("differentMessageByDataGroup", null,
                user);
        assertEquals(notificationList.size(), 1);
        assertEquals(notificationList.get(0).getString("notificationType"), "EARLY");
        assertEquals(notificationList.get(0).getString("message"), MESSAGE_EARLY_2);
    }

    @Test
    public void alreadyNotified() throws Exception {
        // Create user and fake a notification with today's timestamp
        user = createAndInitUser();

        long fakeNotificationTimeMillis = today.toDateTimeAtStartOfDay().getMillis();
        Item item = new Item().withPrimaryKey("userId", user.getUserId(),
                "notificationTime", fakeNotificationTimeMillis)
                .withString("message", "dummy message")
                .withString("notificationType", "EARLY");
        ddbNotificationLogTable.putItem(item);

        // Run test. We only care that the timestamp matches.
        List<Item> notificationList = getNotificationsForUser("alreadyNotified", null, user);
        assertEquals(notificationList.size(), 1);
        assertEquals(notificationList.get(0).getLong("notificationTime"), fakeNotificationTimeMillis);
    }

    private static List<Item> getNotificationsForUser(String testName, LocalDate date, TestUserHelper.TestUser user)
            throws Exception {
        // Execute
        executeNotificationWorker(testName, date);

        // Get notification log for user.
        Iterable<Item> itemIter = ddbNotificationLogTable.query("userId", user.getUserId());
        return ImmutableList.copyOf(itemIter);
    }

    private static void executeNotificationWorker(String testName, LocalDate date) throws Exception {
        System.out.println(DateTime.now(LOCAL_TIME_ZONE).toString() + " Executing Notification Worker for test " +
                testName);

        long previousFinishTime = getLastFinishedTime();

        // Create request
        if (date == null) {
            date = defaultTestDate;
        }
        String requestText = "{\n" +
                "   \"service\":\"ActivityNotificationWorker\",\n" +
                "   \"body\":{\n" +
                "       \"date\":\"" + date.toString() + "\",\n" +
                "       \"studyId\":\"" + IntegTestUtils.STUDY_ID + "\",\n" +
                "       \"tag\":\"Notification Worker Integ Test " + testName + "\"\n" +
                "   }\n" +
                "}";
        ObjectNode requestNode = (ObjectNode) DefaultObjectMapper.INSTANCE.readTree(requestText);
        sqsHelper.sendMessageAsJson(workerSqsUrl, requestNode, 0);

        // Wait until the worker is finished.
        pollWorkerLog(previousFinishTime);
    }

    private static long getLastFinishedTime() {
        // To get the latest notification time, sort the index in reverse and limit the result set to 1.
        QuerySpec query = new QuerySpec()
                .withHashKey("workerId", "ActivityNotificationWorker")
                .withScanIndexForward(false).withMaxResultSize(1);
        Iterator<Item> itemIter = ddbWorkerLogTable.query(query).iterator();
        if (itemIter.hasNext()) {
            Item item = itemIter.next();
            return item.getLong("finishTime");
        } else {
            // Arbitrarily return 0. That's far enough in the past that any reasonable result will be after this.
            return 0;
        }
    }

    // Polls the worker log until the worker is finished, as determined by a new timestamp after the one specified.
    private static void pollWorkerLog(long previousFinishTime) throws Exception {
        long finishTime = previousFinishTime;
        for (int i = 0; i < MAX_POLL_ITERATIONS; i++) {
            Thread.sleep(POLL_DELAY_MILLIS);
            finishTime = getLastFinishedTime();
            if (finishTime > previousFinishTime) {
                break;
            }
        }
        assertTrue(finishTime > previousFinishTime, "Worker log has updated finish time");
    }

    private static TestUserHelper.TestUser createAndInitUser() throws Exception {
        TestUserHelper.TestUser user = createUser();
        initUser(user);
        return user;
    }

    private static TestUserHelper.TestUser createUser() throws Exception {
        SignUp signUp = new SignUp().study(IntegTestUtils.STUDY_ID).phone(IntegTestUtils.PHONE).password("password1");
        signUp.addDataGroupsItem(REQUIRED_DATA_GROUP_1);
        TestUserHelper.TestUser user = TestUserHelper.createAndSignInUser(NotificationTest.class, true,
                signUp);
        return user;
    }

    private static void initUser(TestUserHelper.TestUser user) throws Exception {
        // To initialize user, get activities for the next 28 days. Since get activities is limited to 15 days, do this
        // in 14 day increments.
        DateTime startOfToday = today.toDateTimeAtStartOfDay(LOCAL_TIME_ZONE);
        user.getClient(ActivitiesApi.class).getScheduledActivitiesByDateRange(startOfToday, startOfToday.plusDays(14))
                .execute();
        user.getClient(ActivitiesApi.class).getScheduledActivitiesByDateRange(startOfToday.plusDays(14),
                startOfToday.plusDays(28)).execute();
    }

    private static void completeActivitiesForDateIndices(TestUserHelper.TestUser user, int... indices)
            throws Exception {
        // Calculate get activities date range. Indices are the indices of the dates we care about (as an offset from
        // today). Min date at midnight and max date at 23:59:59.999 to make sure we catch all activities.
        int minIdx = Arrays.stream(indices).min().getAsInt();
        int maxIdx = Arrays.stream(indices).max().getAsInt();
        DateTime todayAtMidnight = today.toDateTimeAtStartOfDay(LOCAL_TIME_ZONE);
        DateTime startTime = todayAtMidnight.plusDays(minIdx).withTimeAtStartOfDay();
        DateTime endTime = todayAtMidnight.plusDays(maxIdx).withHourOfDay(23).withMinuteOfHour(59)
                .withSecondOfMinute(59).withMillisOfSecond(999);

        List<ScheduledActivity> activityList = user.getClient(ActivitiesApi.class).getScheduledActivitiesByDateRange(
                startTime, endTime).execute().body().getItems();

        // Map activities by scheduled date, to make it easier to process. Also, filter out activities with the wrong
        // task ID.
        Map<LocalDate, ScheduledActivity> activitiesByDate = new HashMap<>();
        for (ScheduledActivity oneActivity : activityList) {
            if (TEST_ID.equals(oneActivity.getActivity().getLabel())) {
                LocalDate scheduledDate = oneActivity.getScheduledOn().withZone(LOCAL_TIME_ZONE).toLocalDate();
                activitiesByDate.put(scheduledDate, oneActivity);
            }
        }
        assertTrue(activitiesByDate.size() >= (maxIdx - minIdx + 1));

        // Mark activities as finished.
        List<ScheduledActivity> activitiesToUpdate = new ArrayList<>();
        for (int i : indices) {
            LocalDate scheduledDate = today.plusDays(i);
            ScheduledActivity oneActivity = activitiesByDate.get(scheduledDate);

            // Activities have to be marked as both started and finished to be classified as finished.
            oneActivity.setStartedOn(oneActivity.getScheduledOn().plusHours(1));
            oneActivity.setFinishedOn(oneActivity.getScheduledOn().plusHours(2));
            activitiesToUpdate.add(oneActivity);
        }
        user.getClient(ActivitiesApi.class).updateScheduledActivities(activitiesToUpdate).execute();
    }
}
