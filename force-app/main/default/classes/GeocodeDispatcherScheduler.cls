/**
 * Schedulable safety-net for GeocodeDispatcher. Apex prevents a class from
 * implementing both Queueable and Schedulable simultaneously, so this thin
 * wrapper delegates to GeocodeDispatcher.dispatch() on a periodic cadence.
 *
 * Schedule with e.g.:
 *   System.schedule('Geocode Dispatcher', '0 30 * * * ?', new GeocodeDispatcherScheduler());
 */
public with sharing class GeocodeDispatcherScheduler implements Schedulable {
    public void execute(SchedulableContext context) {
        GeocodeDispatcher.dispatch();
    }
}
