/**
 * Logic for the Message_Queue__c trigger. Kept out of the trigger body so it is
 * unit-testable and bulk-safe. Applies safe defaults on insert and validates that
 * each message carries a well-formed, processable payload before it is committed.
 */
public with sharing class MessageQueueTriggerHandler {

    public static void onBeforeInsert(List<Message_Queue__c> newRecords) {
        for (Message_Queue__c message : newRecords) {
            applyDefaults(message);
            validate(message);
        }
    }

    public static void onBeforeUpdate(List<Message_Queue__c> newRecords, Map<Id, Message_Queue__c> oldMap) {
        for (Message_Queue__c message : newRecords) {
            Message_Queue__c old = oldMap.get(message.Id);
            // Re-validate only when the payload actually changed, to avoid
            // re-parsing on every status flip done by the engine.
            if (message.Payload__c != old.Payload__c) {
                validate(message);
            }
        }
    }

    private static void applyDefaults(Message_Queue__c message) {
        if (String.isBlank(message.Status__c)) {
            message.Status__c = 'Pending';
        }
        if (message.Attempts__c == null) {
            message.Attempts__c = 0;
        }
        if (message.Max_Attempts__c == null) {
            message.Max_Attempts__c = 3;
        }
        if (message.Priority__c == null) {
            message.Priority__c = 5;
        }
        if (message.Scheduled_At__c == null) {
            message.Scheduled_At__c = System.now();
        }
        // Stamp a dedup key so every message is matchable later, even if it was
        // inserted directly rather than through MessageQueueService.enqueue().
        if (String.isBlank(message.Dedup_Key__c) && String.isNotBlank(message.Payload__c)) {
            try {
                message.Dedup_Key__c = MessageQueueService.computeDedupKey(message);
            } catch (Exception e) {
                // Invalid payload is surfaced by validate(); skip key stamping here.
            }
        }
    }

    private static void validate(Message_Queue__c message) {
        if (String.isBlank(message.Payload__c)) {
            message.addError('Payload (JSON) is required.');
            return;
        }
        Map<String, Object> parsed;
        try {
            parsed = (Map<String, Object>) JSON.deserializeUntyped(message.Payload__c);
        } catch (Exception e) {
            message.Payload__c.addError('Payload is not valid JSON: ' + e.getMessage());
            return;
        }
        Object endpoint = parsed.get('endpoint');
        if (endpoint == null || String.isBlank(String.valueOf(endpoint))) {
            message.Payload__c.addError('Payload must include a non-empty "endpoint".');
        }
    }
}
