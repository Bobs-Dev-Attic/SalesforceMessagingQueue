/**
 * Scheduled entry point that drives the message queue once per hour.
 *
 * Run MessageQueueScheduler.start() once (e.g. from anonymous Apex or a post-install
 * script) to register a single Scheduled Apex job whose CRON expression fires at the
 * top of every hour. Scheduled Apex requires integer seconds/minutes in its CRON
 * expression, so a single job cannot fire sub-hourly; for finer cadence register
 * multiple offset jobs or use a self-rescheduling pattern.
 *
 * On each tick it does cheap, synchronous work only: claim a batch of eligible
 * messages (flipping them to 'Processing') and hand the actual HTTP callouts to a
 * Queueable (MessageQueueProcessor), which is permitted to make callouts and runs
 * under its own, larger governor limits.
 */
public with sharing class MessageQueueScheduler implements Schedulable {

    // sec min hour day-of-month month day-of-week -> top of every hour.
    // Salesforce Scheduled Apex requires integer seconds/minutes (no '*'), so a single
    // cron job cannot fire sub-hourly; hourly is the finest single-job cadence.
    public static final String HOURLY_CRON = '0 0 * * * ?';
    public static final String JOB_NAME = 'Message Queue Processor (hourly)';

    // Messages claimed per tick. Bounds the Queueable inside the 100 callouts /
    // transaction governor limit; surplus work is picked up next tick or via chaining.
    @TestVisible private static final Integer DEFAULT_BATCH_SIZE = 100;

    private final Integer batchSize;

    public MessageQueueScheduler() {
        this(DEFAULT_BATCH_SIZE);
    }

    public MessageQueueScheduler(Integer batchSize) {
        this.batchSize = (batchSize == null || batchSize <= 0)
            ? DEFAULT_BATCH_SIZE
            : Math.min(batchSize, 200);
    }

    public void execute(SchedulableContext context) {
        List<Message_Queue__c> claimed = MessageQueueService.claimPending(batchSize);
        if (!claimed.isEmpty()) {
            System.enqueueJob(
                new MessageQueueProcessor(new Map<Id, Message_Queue__c>(claimed).keySet())
            );
        }
    }

    /**
     * Register the every-60-second job. Returns the CronTrigger Id. If a job with
     * the same name already exists the platform throws; callers wanting an
     * idempotent restart should call stop() first.
     */
    public static Id start() {
        return System.schedule(JOB_NAME, HOURLY_CRON, new MessageQueueScheduler());
    }

    /** Abort any scheduled instance(s) of this job. Safe to call when none exist. */
    public static void stop() {
        for (CronTrigger ct : [
            SELECT Id FROM CronTrigger WHERE CronJobDetail.Name = :JOB_NAME
        ]) {
            System.abortJob(ct.Id);
        }
    }
}
