// Anonymous Apex – run once after deploying to register the every-60-second job.
// Re-running is safe: it aborts any existing instance first.
MessageQueueScheduler.stop();
Id jobId = MessageQueueScheduler.start();
System.debug('Message Queue scheduler started: ' + jobId);

// Smoke-test enqueue (optional): drop a message that calls a public echo endpoint.
// Requires a Named Credential named "External_System", or swap for an absolute URL
// plus a Remote Site Setting.
//
// insert MessageQueueService.buildMessage(
//     'GET',
//     'callout:External_System/status',
//     null,
//     new Map<String, String>{ 'Accept' => 'application/json' },
//     null,
//     null
// );
