/**
 * Schedulable safety-net for GeocodeDispatcher. Apex prevents a class from
 * implementing both Queueable and Schedulable simultaneously, so this thin
 * wrapper delegates to GeocodeDispatcher.dispatch() on a periodic cadence.
 *
 * Run GeocodeDispatcherScheduler.start() once (e.g. from anonymous Apex or a
 * post-install script) to register the safety-net job. Call stop() first for
 * an idempotent restart.
 */
public with sharing class GeocodeDispatcherScheduler implements Schedulable {

    public static final String JOB_NAME = 'Geocode Dispatcher (safety-net)';
    // Runs at :30 past every hour, offset from the main queue scheduler at :00 so they don't collide.
    // (Scheduled Apex requires integer seconds/minutes; sub-hourly needs multiple offset jobs.)
    public static final String CRON = '0 30 * * * ?';

    public void execute(SchedulableContext context) {
        GeocodeDispatcher.dispatch();
    }

    /** Register the safety-net job. Returns the CronTrigger Id. Call stop() first for an idempotent restart. */
    public static Id start() {
        return System.schedule(JOB_NAME, CRON, new GeocodeDispatcherScheduler());
    }

    /** Abort any scheduled instance(s) of this job. Safe when none exist. */
    public static void stop() {
        for (CronTrigger ct : [SELECT Id FROM CronTrigger WHERE CronJobDetail.Name = :JOB_NAME]) {
            System.abortJob(ct.Id);
        }
    }
}
