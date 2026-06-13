/**
 * Queueable worker that drains a claimed batch of Message_Queue__c rows.
 *
 * It is enqueued by MessageQueueScheduler (which has already flipped the rows to
 * 'Processing'). Implements Database.AllowsCallouts so it may perform HTTP requests.
 *
 * Two-phase design to satisfy the platform's "no DML between callouts" rule:
 *   Phase 1 - run every callout, buffering responses / exceptions in memory.
 *   Phase 2 - interpret responses, dispatch to response handlers, build updates,
 *             then commit a single DML.
 * Finally it chains a fresh processor if more work is waiting, so a backlog drains
 * faster than the once-per-minute scheduler cadence.
 */
public with sharing class MessageQueueProcessor implements Queueable, Database.AllowsCallouts {

    @TestVisible private static final Integer CHAIN_BATCH_SIZE = 100;

    private final Set<Id> messageIds;

    public MessageQueueProcessor(Set<Id> messageIds) {
        this.messageIds = messageIds == null ? new Set<Id>() : messageIds;
    }

    public void execute(QueueableContext context) {
        List<Message_Queue__c> messages = [
            SELECT Id, Status__c, Payload__c, Response_Handler__c,
                   Attempts__c, Max_Attempts__c, HTTP_Method__c, Endpoint__c,
                   Related_Record_Id__c
            FROM Message_Queue__c
            WHERE Id IN :messageIds AND Status__c = 'Processing'
            FOR UPDATE
        ];
        if (messages.isEmpty()) {
            return;
        }

        Map<Id, HttpResponse> responses = new Map<Id, HttpResponse>();
        Map<Id, String> calloutErrors = new Map<Id, String>();

        // Phase 1 - callouts only. No DML may run until every callout is done.
        for (Message_Queue__c message : messages) {
            try {
                responses.put(message.Id, MessageQueueService.executeCallout(message));
            } catch (Exception e) {
                calloutErrors.put(message.Id, e.getMessage());
            }
        }

        // Phase 2 - interpret results (DML / handler logic is safe now).
        for (Message_Queue__c message : messages) {
            message.Attempts__c = (message.Attempts__c == null ? 0 : message.Attempts__c) + 1;
            try {
                if (calloutErrors.containsKey(message.Id)) {
                    MessageQueueService.markFailureOrRetry(message, calloutErrors.get(message.Id));
                } else {
                    MessageQueueService.handleResponse(message, responses.get(message.Id));
                }
            } catch (Exception e) {
                // A bad payload or a throwing response handler must not roll back
                // the rest of the batch; record it against this message only.
                MessageQueueService.markFailureOrRetry(message, e.getMessage());
            }
        }

        update messages;

        chainIfMoreWork();
    }

    /**
     * Claim and enqueue the next batch if more eligible work remains. Skipped in
     * tests (Queueable chaining is capped at depth 1 in test context) and when the
     * async-Apex flex queue is likely saturated.
     */
    private void chainIfMoreWork() {
        if (Test.isRunningTest()) {
            return;
        }
        if (Limits.getQueueableJobs() >= Limits.getLimitQueueableJobs()) {
            return;
        }
        List<Message_Queue__c> next = MessageQueueService.claimPending(CHAIN_BATCH_SIZE);
        if (!next.isEmpty()) {
            System.enqueueJob(new MessageQueueProcessor(new Map<Id, Message_Queue__c>(next).keySet()));
        }
    }
}
