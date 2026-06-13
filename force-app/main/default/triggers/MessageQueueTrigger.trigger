/**
 * Guards the queue object itself: defaults new messages to a processable state and
 * validates their JSON payload before commit. Delegates to MessageQueueTriggerHandler.
 */
trigger MessageQueueTrigger on Message_Queue__c (before insert, before update) {
    if (Trigger.isInsert) {
        MessageQueueTriggerHandler.onBeforeInsert(Trigger.new);
    } else if (Trigger.isUpdate) {
        MessageQueueTriggerHandler.onBeforeUpdate(Trigger.new, Trigger.oldMap);
    }
}
