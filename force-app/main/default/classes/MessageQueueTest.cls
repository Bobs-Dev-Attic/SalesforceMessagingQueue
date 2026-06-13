/**
 * Unit tests for the message queue: producer/guard triggers, claiming, the
 * Queueable processor (success / custom handler / retry / permanent failure),
 * and scheduler registration.
 */
@IsTest
private class MessageQueueTest {

    private static Message_Queue__c newPending(String endpoint, String handler, Integer maxAttempts) {
        Message_Queue__c m = MessageQueueService.buildMessage(
            'POST', endpoint, new Map<String, Object>{ 'k' => 'v' },
            new Map<String, String>{ 'Content-Type' => 'application/json' },
            handler, null
        );
        m.Max_Attempts__c = maxAttempts;
        return m;
    }

    // ---- Guard trigger: defaults + validation ----------------------------

    @IsTest
    static void appliesDefaultsOnInsert() {
        Message_Queue__c m = new Message_Queue__c(
            Payload__c = '{"endpoint":"callout:X/y","method":"GET"}'
        );
        insert m;

        m = [SELECT Status__c, Attempts__c, Max_Attempts__c, Priority__c, Scheduled_At__c
             FROM Message_Queue__c WHERE Id = :m.Id];
        System.assertEquals('Pending', m.Status__c, 'Should default to Pending');
        System.assertEquals(0, m.Attempts__c, 'Attempts should default to 0');
        System.assertEquals(3, m.Max_Attempts__c, 'Max attempts should default to 3');
        System.assertEquals(5, m.Priority__c, 'Priority should default to 5');
        System.assertNotEquals(null, m.Scheduled_At__c, 'Scheduled_At should be defaulted');
    }

    @IsTest
    static void rejectsInvalidJsonPayload() {
        try {
            insert new Message_Queue__c(Payload__c = 'not-json');
            System.assert(false, 'Expected a validation error for invalid JSON');
        } catch (DmlException e) {
            System.assert(e.getMessage().contains('not valid JSON'), e.getMessage());
        }
    }

    @IsTest
    static void rejectsPayloadWithoutEndpoint() {
        try {
            insert new Message_Queue__c(Payload__c = '{"method":"GET"}');
            System.assert(false, 'Expected a validation error for missing endpoint');
        } catch (DmlException e) {
            System.assert(e.getMessage().contains('endpoint'), e.getMessage());
        }
    }

    // ---- Claiming --------------------------------------------------------

    @IsTest
    static void claimPendingFlipsToProcessingInPriorityOrder() {
        Message_Queue__c high = newPending('callout:X/high', null, 3);
        high.Priority__c = 1;
        Message_Queue__c low = newPending('callout:X/low', null, 3);
        low.Priority__c = 9;
        insert new List<Message_Queue__c>{ low, high };

        Test.startTest();
        List<Message_Queue__c> claimed = MessageQueueService.claimPending(1);
        Test.stopTest();

        System.assertEquals(1, claimed.size(), 'Limit should be respected');
        System.assertEquals(high.Id, claimed[0].Id, 'Highest priority (lowest number) claimed first');
        System.assertEquals('Processing',
            [SELECT Status__c FROM Message_Queue__c WHERE Id = :high.Id].Status__c);
    }

    // ---- Processor: happy path ------------------------------------------

    @IsTest
    static void processorMarksCompletedOnSuccess() {
        Message_Queue__c m = newPending('callout:X/ok', null, 3);
        insert m;

        Test.setMock(HttpCalloutMock.class, new MessageQueueMock(200, '{"id":"abc"}'));
        Test.startTest();
        new MessageQueueScheduler().execute(null); // claims + enqueues processor
        Test.stopTest();                            // runs the Queueable

        m = [SELECT Status__c, Attempts__c, Response_Status_Code__c, Response_Body__c, Processed_At__c
             FROM Message_Queue__c WHERE Id = :m.Id];
        System.assertEquals('Completed', m.Status__c);
        System.assertEquals(1, m.Attempts__c);
        System.assertEquals(200, m.Response_Status_Code__c);
        System.assertEquals('{"id":"abc"}', m.Response_Body__c);
        System.assertNotEquals(null, m.Processed_At__c);
    }

    // ---- Processor: custom handler overrides default decision ------------

    @IsTest
    static void customHandlerCanTreatNonSuccessAsCompleted() {
        Message_Queue__c m = newPending('callout:X/dup', 'AccountSyncResponseHandler', 3);
        insert m;

        // 409 is NOT a default success code, but AccountSyncResponseHandler maps it
        // to Completed.
        Test.setMock(HttpCalloutMock.class, new MessageQueueMock(409, '{"id":"remote-1"}'));
        Test.startTest();
        new MessageQueueScheduler().execute(null);
        Test.stopTest();

        System.assertEquals('Completed',
            [SELECT Status__c FROM Message_Queue__c WHERE Id = :m.Id].Status__c);
    }

    // ---- Processor: retry then permanent failure -------------------------

    @IsTest
    static void serverErrorSchedulesRetryWhenAttemptsRemain() {
        Message_Queue__c m = newPending('callout:X/err', null, 3);
        insert m;

        Test.setMock(HttpCalloutMock.class, new MessageQueueMock(500, 'boom'));
        Test.startTest();
        new MessageQueueScheduler().execute(null);
        Test.stopTest();

        m = [SELECT Status__c, Attempts__c, Next_Attempt_At__c, Error_Message__c
             FROM Message_Queue__c WHERE Id = :m.Id];
        System.assertEquals('Retry', m.Status__c, 'Should retry while attempts remain');
        System.assertEquals(1, m.Attempts__c);
        System.assertNotEquals(null, m.Next_Attempt_At__c, 'Backoff time should be set');
    }

    @IsTest
    static void serverErrorMarksFailedWhenAttemptsExhausted() {
        Message_Queue__c m = newPending('callout:X/err', null, 1);
        insert m;

        Test.setMock(HttpCalloutMock.class, new MessageQueueMock(503, 'down'));
        Test.startTest();
        new MessageQueueScheduler().execute(null);
        Test.stopTest();

        m = [SELECT Status__c, Attempts__c FROM Message_Queue__c WHERE Id = :m.Id];
        System.assertEquals('Failed', m.Status__c, 'Should fail once max attempts reached');
        System.assertEquals(1, m.Attempts__c);
    }

    // ---- Producer trigger ------------------------------------------------

    @IsTest
    static void accountTriggerEnqueuesMessage() {
        Test.startTest();
        Account acc = new Account(Name = 'Acme Inc');
        insert acc;
        Test.stopTest();

        List<Message_Queue__c> msgs = [
            SELECT Status__c, Related_Record_Id__c, Payload__c, HTTP_Method__c
            FROM Message_Queue__c WHERE Related_Record_Id__c = :acc.Id
        ];
        System.assertEquals(1, msgs.size(), 'Account insert should enqueue one message');
        System.assertEquals('Pending', msgs[0].Status__c);
        System.assertEquals('POST', msgs[0].HTTP_Method__c);
        System.assert(msgs[0].Payload__c.contains('accounts'), 'Endpoint should be in payload');
    }

    // ---- Deduplication ---------------------------------------------------

    @IsTest
    static void enqueueSuppressesDuplicateWithinWindow() {
        MessageQueueService.dedupEnabledOverride = true;
        MessageQueueService.dedupWindowOverride = 300;

        MessageQueueService.EnqueueResult first = MessageQueueService.enqueue(
            new List<Message_Queue__c>{ newPending('callout:X/dup', null, 3) }
        );
        MessageQueueService.EnqueueResult second = MessageQueueService.enqueue(
            new List<Message_Queue__c>{ newPending('callout:X/dup', null, 3) }
        );

        System.assertEquals(1, first.inserted.size());
        System.assertEquals(0, first.skipped.size());
        System.assertEquals(0, second.inserted.size(), 'Second identical call should be suppressed');
        System.assertEquals(1, second.skipped.size());
        System.assertEquals(1, [SELECT COUNT() FROM Message_Queue__c]);
    }

    @IsTest
    static void enqueueDedupesWithinSingleBatch() {
        MessageQueueService.dedupEnabledOverride = true;
        MessageQueueService.dedupWindowOverride = 300;

        MessageQueueService.EnqueueResult result = MessageQueueService.enqueue(new List<Message_Queue__c>{
            newPending('callout:X/batch', null, 3),
            newPending('callout:X/batch', null, 3)
        });

        System.assertEquals(1, result.inserted.size(), 'Only one of two identical messages inserts');
        System.assertEquals(1, result.skipped.size());
    }

    @IsTest
    static void jsonDedupSecondsZeroDisablesDedup() {
        MessageQueueService.dedupEnabledOverride = true;
        MessageQueueService.dedupWindowOverride = 300;

        // dedupSeconds = 0 in the payload overrides the global window and turns dedup off,
        // so two otherwise-identical calls both get inserted.
        MessageQueueService.enqueue(buildWithDedupSeconds('callout:X/off', 0));
        MessageQueueService.EnqueueResult second =
            MessageQueueService.enqueue(buildWithDedupSeconds('callout:X/off', 0));

        System.assertEquals(1, second.inserted.size(), 'dedupSeconds=0 should disable dedup');
        System.assertEquals(0, second.skipped.size());
        System.assertEquals(2, [SELECT COUNT() FROM Message_Queue__c]);
    }

    @IsTest
    static void jsonDedupSecondsForcesDedupWhenGloballyDisabled() {
        MessageQueueService.dedupEnabledOverride = false; // globally off
        MessageQueueService.dedupWindowOverride = 0;

        MessageQueueService.MessagePayload p1 = new MessageQueueService.MessagePayload();
        p1.method = 'POST'; p1.endpoint = 'callout:X/force'; p1.dedupSeconds = 300;
        MessageQueueService.MessagePayload p2 = new MessageQueueService.MessagePayload();
        p2.method = 'POST'; p2.endpoint = 'callout:X/force'; p2.dedupSeconds = 300;

        MessageQueueService.enqueue(MessageQueueService.buildMessage(p1, null, null));
        MessageQueueService.EnqueueResult second =
            MessageQueueService.enqueue(MessageQueueService.buildMessage(p2, null, null));

        System.assertEquals(0, second.inserted.size(), 'Payload override should force dedup on');
        System.assertEquals(1, second.skipped.size());
    }

    @IsTest
    static void failedMessageDoesNotBlockReenqueue() {
        MessageQueueService.dedupEnabledOverride = true;
        MessageQueueService.dedupWindowOverride = 300;

        Message_Queue__c original = newPending('callout:X/failed', null, 3);
        MessageQueueService.enqueue(new List<Message_Queue__c>{ original });
        original.Status__c = 'Failed';
        update original;

        MessageQueueService.EnqueueResult retry = MessageQueueService.enqueue(
            new List<Message_Queue__c>{ newPending('callout:X/failed', null, 3) }
        );
        System.assertEquals(1, retry.inserted.size(), 'A failed prior call must not block a fresh enqueue');
    }

    private static Message_Queue__c buildWithDedupSeconds(String endpoint, Integer seconds) {
        MessageQueueService.MessagePayload p = new MessageQueueService.MessagePayload();
        p.method = 'POST';
        p.endpoint = endpoint;
        p.dedupSeconds = seconds;
        return MessageQueueService.buildMessage(p, null, null);
    }

    // ---- Scheduler registration -----------------------------------------

    @IsTest
    static void schedulerRegistersAndAborts() {
        Test.startTest();
        Id jobId = MessageQueueScheduler.start();
        Test.stopTest();

        System.assertNotEquals(null, jobId, 'A scheduled job id should be returned');
        Integer count = [
            SELECT COUNT() FROM CronTrigger
            WHERE CronJobDetail.Name = :MessageQueueScheduler.JOB_NAME
        ];
        System.assertEquals(1, count, 'Exactly one queue scheduler job should exist');

        MessageQueueScheduler.stop();
        count = [
            SELECT COUNT() FROM CronTrigger
            WHERE CronJobDetail.Name = :MessageQueueScheduler.JOB_NAME
        ];
        System.assertEquals(0, count, 'Scheduler job should be aborted');
    }
}
