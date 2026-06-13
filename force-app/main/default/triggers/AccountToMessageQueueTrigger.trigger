/**
 * EXAMPLE producer trigger.
 *
 * Demonstrates how a business object enqueues outbound integration work: when an
 * Account is created or its Name changes, it drops a JSON-described message onto
 * the queue to sync the record with an external system. The scheduled processor
 * (MessageQueueScheduler -> MessageQueueProcessor) makes the actual callout 60s
 * later, so the originating DML transaction stays fast and never blocks on HTTP.
 *
 * The payload's "responseHandler" routes the reply to AccountSyncResponseHandler.
 * The "endpoint" uses a Named Credential (callout:External_System) so no secrets
 * or raw URLs live in code. Replace the endpoint/handler to fit your integration,
 * or copy this pattern onto any other SObject.
 */
trigger AccountToMessageQueueTrigger on Account (after insert, after update) {

    List<Message_Queue__c> messages = new List<Message_Queue__c>();

    for (Account acc : Trigger.new) {
        Boolean changed = Trigger.isInsert
            || acc.Name != Trigger.oldMap.get(acc.Id).Name;
        if (!changed) {
            continue;
        }

        Map<String, Object> body = new Map<String, Object>{
            'salesforceId' => acc.Id,
            'name'         => acc.Name,
            'event'        => Trigger.isInsert ? 'created' : 'updated'
        };

        messages.add(
            MessageQueueService.buildMessage(
                'POST',                                       // HTTP method
                'callout:External_System/api/v1/accounts',    // Named Credential endpoint
                body,                                         // request body (serialized to JSON)
                new Map<String, String>{ 'Content-Type' => 'application/json' },
                'AccountSyncResponseHandler',                 // how to process the response
                acc.Id                                        // related record
            )
        );
    }

    if (!messages.isEmpty()) {
        insert messages;
    }
}
