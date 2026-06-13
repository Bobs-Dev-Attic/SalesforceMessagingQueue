/**
 * EXAMPLE producer trigger for the Census geocoding use case.
 *
 * On insert of Pending Geocode_Request__c records it enqueues a single
 * GeocodeDispatcher job (deduped per transaction). The dispatcher decides, by
 * volume, whether to issue individual GET callouts or batch CSV uploads to the
 * Census geocoder, then the scheduled processor performs the callout(s).
 *
 * Routing lives in GeocodeDispatcher (not here) because a bulk insert arrives in
 * 200-record trigger chunks and cannot see the total pending volume.
 */
trigger GeocodeRequestTrigger on Geocode_Request__c (after insert) {

    if (GeocodeDispatcher.dispatchQueued) {
        return; // already enqueued (or suppressed by a test) in this transaction
    }

    for (Geocode_Request__c req : Trigger.new) {
        if (req.Geocode_Status__c == 'Pending') {
            GeocodeDispatcher.dispatchQueued = true;
            System.enqueueJob(new GeocodeDispatcher());
            break;
        }
    }
}
