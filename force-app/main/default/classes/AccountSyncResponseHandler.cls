/**
 * EXAMPLE response handler referenced by AccountToMessageQueueTrigger.
 *
 * Shows how the "how to process the response" half of a message works: parse the
 * external system's JSON reply, write results back to Salesforce, and (optionally)
 * override the queue engine's default success/failure decision by setting
 * message.Status__c. Here we treat a 409 (already exists) as success and store the
 * remote id the external system returned.
 *
 * Runs in the processor's callout transaction after all callouts complete: DML is
 * allowed here, further callouts are NOT.
 */
public with sharing class AccountSyncResponseHandler implements MessageQueueResponseHandler {

    public void handle(Message_Queue__c message, HttpResponse response) {
        Integer status = response.getStatusCode();

        // 409 = the account already exists remotely; treat as a successful sync.
        if (status == 200 || status == 201 || status == 409) {
            String remoteId = extractRemoteId(response.getBody());
            if (String.isNotBlank(remoteId) && String.isNotBlank(message.Related_Record_Id__c)) {
                // Persist the external id back onto the originating Account. Replace
                // External_Id__c with the field that exists in your org.
                // update new Account(Id = message.Related_Record_Id__c, External_Id__c = remoteId);
            }
            MessageQueueService.markCompleted(message);
        }
        // Any other status: leave Status__c as 'Processing' so the engine applies
        // its default retry-with-backoff / failure handling.
    }

    private String extractRemoteId(String body) {
        if (String.isBlank(body)) {
            return null;
        }
        try {
            Map<String, Object> parsed = (Map<String, Object>) JSON.deserializeUntyped(body);
            Object id = parsed.get('id');
            return id == null ? null : String.valueOf(id);
        } catch (Exception e) {
            return null;
        }
    }
}
