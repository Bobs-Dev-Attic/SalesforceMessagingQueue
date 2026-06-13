/**
 * Strategy interface for interpreting the HTTP response of a queued message.
 *
 * The payload's "responseHandler" property (or the Message_Queue__c.Response_Handler__c
 * fallback) names a concrete Apex class implementing this interface. The processor
 * instantiates it dynamically (Type.forName) and calls handle() after the callout
 * returns, so new integrations can plug in custom response logic without touching
 * the queue engine.
 *
 * IMPORTANT: handle() runs in the callout transaction AFTER all callouts for the
 * batch have completed. It may perform DML, but it must NOT perform further
 * callouts. To override the queue engine's default success/failure decision, set
 * message.Status__c directly (e.g. to 'Completed', 'Failed', or 'Retry'); the
 * engine only applies its own decision when the status is still 'Processing'.
 */
public interface MessageQueueResponseHandler {
    void handle(Message_Queue__c message, HttpResponse response);
}
