/**
 * Core engine for the durable outbound message queue.
 *
 * Responsibilities:
 *   - Build & enqueue Message_Queue__c records from triggers / services.
 *   - Atomically claim Pending/Retry messages for processing (claimPending).
 *   - Execute the HTTP callout described by a message's JSON payload.
 *   - Interpret the response, dispatch to a pluggable MessageQueueResponseHandler,
 *     and apply success / retry-with-backoff / failure transitions.
 *
 * The class deliberately separates the callout phase from the DML phase so the
 * Queueable processor can honour the platform rule that no DML may be committed
 * between callouts in the same transaction.
 */
public with sharing class MessageQueueService {

    public class MessageQueueException extends Exception {}

    @TestVisible private static final Integer DEFAULT_MAX_ATTEMPTS = 3;
    @TestVisible private static final Integer DEFAULT_TIMEOUT_MS = 30000;
    @TestVisible private static final Integer MAX_TEXT_AREA = 131072;
    @TestVisible private static final Integer MAX_ERROR_LEN = 32768;

    // Fallback dedup config when no Message_Queue_Config__mdt.Default record exists.
    @TestVisible private static final Integer FALLBACK_DEDUP_WINDOW_SECONDS = 300;

    // Statuses that still count as an active/recent call for dedup purposes. Failed
    // and Cancelled messages do NOT suppress a fresh enqueue.
    private static final Set<String> DEDUP_BLOCKING_STATUSES =
        new Set<String>{ 'Pending', 'Processing', 'Retry', 'Completed' };

    // Test hooks so dedup behaviour can be exercised without editing the CMDT record.
    @TestVisible private static Boolean dedupEnabledOverride;
    @TestVisible private static Integer dedupWindowOverride;

    /**
     * Typed view of the JSON instruction stored in Message_Queue__c.Payload__c.
     *
     * Example payload:
     * {
     *   "method": "POST",
     *   "endpoint": "callout:External_System/api/v1/accounts",
     *   "headers": { "Content-Type": "application/json", "X-Source": "SFDC" },
     *   "body": { "name": "Acme", "externalRef": "001..." },
     *   "timeout": 20000,
     *   "successStatusCodes": [200, 201, 202],
     *   "responseHandler": "AccountSyncResponseHandler"
     * }
     */
    public class MessagePayload {
        public String method;                 // HTTP verb; defaults to GET
        public String endpoint;               // required; callout:NC/... or absolute URL
        public Map<String, String> headers;   // optional request headers
        public Object body;                    // optional; String sent as-is, otherwise JSON-serialized
        public Integer timeout;                // optional; ms (1 - 120000)
        public List<Integer> successStatusCodes; // optional; defaults to 2xx
        public String responseHandler;         // optional; Apex class implementing MessageQueueResponseHandler
        public Integer dedupSeconds;           // optional; overrides the dedup window. 0 disables dedup; >0 forces it on
        public String dedupKey;                // optional; explicit dedup key (else a hash of method+endpoint+body)
    }

    // ---------------------------------------------------------------------
    // Enqueue helpers (used by triggers / application code)
    // ---------------------------------------------------------------------

    /**
     * Construct (but do not insert) a queue message from a payload object.
     * The caller is responsible for the DML so messages can be inserted in bulk.
     */
    public static Message_Queue__c buildMessage(MessagePayload payload, String relatedRecordId, String correlationId) {
        if (payload == null || String.isBlank(payload.endpoint)) {
            throw new MessageQueueException('A message payload with an endpoint is required.');
        }
        payload.method = normalizeMethod(payload.method);
        return new Message_Queue__c(
            Payload__c = JSON.serialize(payload, true),
            HTTP_Method__c = payload.method,
            Endpoint__c = payload.endpoint.abbreviate(255),
            Response_Handler__c = payload.responseHandler,
            Related_Record_Id__c = relatedRecordId,
            Correlation_Id__c = correlationId,
            Status__c = 'Pending'
        );
    }

    /** Convenience overload for the most common call shape. */
    public static Message_Queue__c buildMessage(
        String method,
        String endpoint,
        Object body,
        Map<String, String> headers,
        String responseHandler,
        String relatedRecordId
    ) {
        MessagePayload payload = new MessagePayload();
        payload.method = method;
        payload.endpoint = endpoint;
        payload.body = body;
        payload.headers = headers;
        payload.responseHandler = responseHandler;
        return buildMessage(payload, relatedRecordId, null);
    }

    // ---------------------------------------------------------------------
    // Enqueue with windowed deduplication
    // ---------------------------------------------------------------------

    /** Outcome of an enqueue: which messages were inserted and which were suppressed as duplicates. */
    public class EnqueueResult {
        public List<Message_Queue__c> inserted = new List<Message_Queue__c>();
        public List<Message_Queue__c> skipped = new List<Message_Queue__c>();
    }

    /** Convenience single-record overload. */
    public static EnqueueResult enqueue(Message_Queue__c message) {
        return enqueue(new List<Message_Queue__c>{ message });
    }

    /**
     * Insert messages, silently suppressing any whose dedup key matches a
     * non-failed message enqueued within the configured window (and de-duping
     * identical messages within this same batch). Bulk-safe: one query, one insert.
     *
     * Window resolution per message: the payload's "dedupSeconds" wins (0 disables,
     * >0 forces on); otherwise the Message_Queue_Config__mdt default applies.
     */
    public static EnqueueResult enqueue(List<Message_Queue__c> messages) {
        EnqueueResult result = new EnqueueResult();
        if (messages == null || messages.isEmpty()) {
            return result;
        }

        // Resolve dedup config; split into "always insert" vs dedup candidates.
        // NOTE: parallel lists (not an SObject-keyed map) because Apex treats
        // field-identical SObjects as equal map keys, which would drop messages.
        List<Message_Queue__c> candidates = new List<Message_Queue__c>();
        List<Integer> candidateWindows = new List<Integer>();
        Set<String> keys = new Set<String>();
        Integer maxWindow = 0;
        for (Message_Queue__c message : messages) {
            DedupConfig cfg = resolveDedup(message);
            if (!cfg.enabled) {
                result.inserted.add(message); // dedup off for this message
                continue;
            }
            message.Dedup_Key__c = cfg.key;
            message.Dedup_Window_Seconds__c = cfg.windowSeconds;
            candidates.add(message);
            candidateWindows.add(cfg.windowSeconds);
            keys.add(cfg.key);
            maxWindow = Math.max(maxWindow, cfg.windowSeconds);
        }

        // Most-recent blocking message per key within the widest window in play.
        Map<String, Datetime> latestByKey = new Map<String, Datetime>();
        if (!keys.isEmpty()) {
            Datetime earliest = System.now().addSeconds(-maxWindow);
            for (Message_Queue__c existing : [
                SELECT Dedup_Key__c, CreatedDate
                FROM Message_Queue__c
                WHERE Dedup_Key__c IN :keys
                  AND Status__c IN :DEDUP_BLOCKING_STATUSES
                  AND CreatedDate >= :earliest
                ORDER BY CreatedDate DESC
            ]) {
                if (!latestByKey.containsKey(existing.Dedup_Key__c)) {
                    latestByKey.put(existing.Dedup_Key__c, existing.CreatedDate);
                }
            }
        }

        // Evaluate each candidate against the DB and against earlier rows in this batch.
        Datetime now = System.now();
        Set<String> seenInBatch = new Set<String>();
        for (Integer i = 0; i < candidates.size(); i++) {
            Message_Queue__c message = candidates[i];
            Datetime threshold = now.addSeconds(-candidateWindows[i]);
            Datetime existingWhen = latestByKey.get(message.Dedup_Key__c);
            Boolean duplicate = seenInBatch.contains(message.Dedup_Key__c)
                || (existingWhen != null && existingWhen >= threshold);
            if (duplicate) {
                result.skipped.add(message);
            } else {
                seenInBatch.add(message.Dedup_Key__c);
                result.inserted.add(message);
            }
        }

        if (!result.inserted.isEmpty()) {
            insert result.inserted;
        }
        return result;
    }

    /** Public so the trigger can stamp a dedup key on directly-inserted messages. */
    public static String computeDedupKey(Message_Queue__c message) {
        return resolveDedup(message).key;
    }

    // ---------------------------------------------------------------------
    // Claiming work
    // ---------------------------------------------------------------------

    /**
     * Atomically move up to limitSize eligible messages to 'Processing' and return
     * them. FOR UPDATE row-locking prevents two overlapping scheduler ticks from
     * grabbing the same rows. Runs in the (synchronous) scheduler transaction with
     * no callouts, so the lock is held only briefly.
     */
    public static List<Message_Queue__c> claimPending(Integer limitSize) {
        Datetime now = System.now();
        List<Message_Queue__c> toClaim = [
            SELECT Id
            FROM Message_Queue__c
            WHERE Status__c IN ('Pending', 'Retry')
              AND (Scheduled_At__c = null OR Scheduled_At__c <= :now)
              AND (Next_Attempt_At__c = null OR Next_Attempt_At__c <= :now)
            ORDER BY Priority__c ASC NULLS LAST, Scheduled_At__c ASC NULLS FIRST, CreatedDate ASC
            LIMIT :limitSize
            FOR UPDATE
        ];
        for (Message_Queue__c m : toClaim) {
            m.Status__c = 'Processing';
        }
        if (!toClaim.isEmpty()) {
            update toClaim;
        }
        return toClaim;
    }

    // ---------------------------------------------------------------------
    // Callout + response handling
    // ---------------------------------------------------------------------

    /** Perform the HTTP callout described by the message payload. */
    public static HttpResponse executeCallout(Message_Queue__c message) {
        MessagePayload payload = parsePayload(message);

        HttpRequest request = new HttpRequest();
        request.setEndpoint(payload.endpoint);
        request.setMethod(payload.method);
        request.setTimeout(resolveTimeout(payload.timeout));

        if (payload.headers != null) {
            for (String key : payload.headers.keySet()) {
                request.setHeader(key, payload.headers.get(key));
            }
        }

        String body = bodyToString(payload.body);
        if (String.isNotBlank(body) && payload.method != 'GET' && payload.method != 'DELETE') {
            request.setBody(body);
        }

        return new Http().send(request);
    }

    /**
     * Interpret the response for a single message and set its terminal state.
     * Assumes message.Attempts__c has already been incremented by the caller.
     * Mutates the record in memory only; the caller commits the DML.
     */
    public static void handleResponse(Message_Queue__c message, HttpResponse response) {
        message.Response_Status_Code__c = response.getStatusCode();
        message.Response_Body__c = abbreviate(response.getBody(), MAX_TEXT_AREA);

        MessagePayload payload = parsePayload(message);
        Boolean success = isSuccess(response.getStatusCode(), payload.successStatusCodes);

        // Dispatch to a custom handler if one is configured. It may override the
        // outcome by setting message.Status__c directly.
        String handlerName = String.isNotBlank(payload.responseHandler)
            ? payload.responseHandler
            : message.Response_Handler__c;
        if (String.isNotBlank(handlerName)) {
            instantiateHandler(handlerName).handle(message, response);
        }

        // Apply the default decision only if the handler left it 'Processing'.
        if (message.Status__c == 'Processing') {
            if (success) {
                markCompleted(message);
            } else {
                markFailureOrRetry(
                    message,
                    'HTTP ' + response.getStatusCode() + ': ' + abbreviate(response.getBody(), 500)
                );
            }
        }
    }

    /** Move a message to its successful terminal state. */
    public static void markCompleted(Message_Queue__c message) {
        message.Status__c = 'Completed';
        message.Processed_At__c = System.now();
        message.Error_Message__c = null;
    }

    /**
     * Record a failed attempt: either schedule a backoff retry or, once attempts
     * are exhausted, mark the message permanently Failed.
     */
    public static void markFailureOrRetry(Message_Queue__c message, String error) {
        message.Error_Message__c = abbreviate(error, MAX_ERROR_LEN);
        Decimal attempts = message.Attempts__c == null ? 0 : message.Attempts__c;
        Decimal maxAttempts = message.Max_Attempts__c == null ? DEFAULT_MAX_ATTEMPTS : message.Max_Attempts__c;

        if (attempts >= maxAttempts) {
            message.Status__c = 'Failed';
        } else {
            message.Status__c = 'Retry';
            // Exponential backoff: 1, 2, 4, 8 ... minutes (capped at ~17 min).
            Integer exponent = Math.min(attempts.intValue() - 1, 10);
            Integer delayMinutes = (Integer) Math.pow(2, Math.max(exponent, 0));
            message.Next_Attempt_At__c = System.now().addMinutes(delayMinutes);
        }
    }

    // ---------------------------------------------------------------------
    // Internals
    // ---------------------------------------------------------------------

    private class DedupConfig {
        Boolean enabled;
        String key;
        Integer windowSeconds;
    }

    /** Derive the dedup key, window, and on/off state for a message from its payload + config. */
    private static DedupConfig resolveDedup(Message_Queue__c message) {
        MessagePayload payload = parsePayload(message);
        DedupConfig cfg = new DedupConfig();

        // Key: explicit override, else SHA-256 of the call's identifying parts.
        if (String.isNotBlank(payload.dedupKey)) {
            cfg.key = payload.dedupKey.abbreviate(255);
        } else {
            String raw = payload.method + '|' + payload.endpoint + '|' + bodyToString(payload.body);
            cfg.key = EncodingUtil.convertToHex(Crypto.generateDigest('SHA-256', Blob.valueOf(raw)));
        }

        // Window: per-message override wins (0 disables, >0 forces on); else config default.
        if (payload.dedupSeconds != null) {
            cfg.windowSeconds = payload.dedupSeconds;
            cfg.enabled = payload.dedupSeconds > 0;
        } else {
            cfg.windowSeconds = defaultDedupWindowSeconds();
            cfg.enabled = globalDedupEnabled() && cfg.windowSeconds > 0;
        }
        return cfg;
    }

    private static Boolean globalDedupEnabled() {
        if (dedupEnabledOverride != null) {
            return dedupEnabledOverride;
        }
        Message_Queue_Config__mdt config = Message_Queue_Config__mdt.getInstance('Default');
        return config == null ? true : config.Dedup_Enabled__c;
    }

    private static Integer defaultDedupWindowSeconds() {
        if (dedupWindowOverride != null) {
            return dedupWindowOverride;
        }
        Message_Queue_Config__mdt config = Message_Queue_Config__mdt.getInstance('Default');
        return (config != null && config.Default_Dedup_Window_Seconds__c != null)
            ? config.Default_Dedup_Window_Seconds__c.intValue()
            : FALLBACK_DEDUP_WINDOW_SECONDS;
    }

    @TestVisible
    private static MessagePayload parsePayload(Message_Queue__c message) {
        if (String.isBlank(message.Payload__c)) {
            throw new MessageQueueException('Message ' + message.Id + ' has an empty payload.');
        }
        MessagePayload payload;
        try {
            payload = (MessagePayload) JSON.deserialize(message.Payload__c, MessagePayload.class);
        } catch (Exception e) {
            throw new MessageQueueException('Message ' + message.Id + ' payload is not valid JSON: ' + e.getMessage());
        }
        if (String.isBlank(payload.endpoint)) {
            throw new MessageQueueException('Message ' + message.Id + ' payload is missing "endpoint".');
        }
        payload.method = normalizeMethod(payload.method);
        return payload;
    }

    @TestVisible
    private static Boolean isSuccess(Integer statusCode, List<Integer> successCodes) {
        if (successCodes != null && !successCodes.isEmpty()) {
            return successCodes.contains(statusCode);
        }
        return statusCode >= 200 && statusCode < 300;
    }

    private static MessageQueueResponseHandler instantiateHandler(String className) {
        Type handlerType = Type.forName(className);
        if (handlerType == null) {
            throw new MessageQueueException('Unknown response handler class: ' + className);
        }
        Object instance = handlerType.newInstance();
        if (!(instance instanceof MessageQueueResponseHandler)) {
            throw new MessageQueueException(className + ' must implement MessageQueueResponseHandler.');
        }
        return (MessageQueueResponseHandler) instance;
    }

    private static String normalizeMethod(String method) {
        return String.isBlank(method) ? 'GET' : method.trim().toUpperCase();
    }

    private static Integer resolveTimeout(Integer timeout) {
        if (timeout == null) {
            return DEFAULT_TIMEOUT_MS;
        }
        return Math.min(Math.max(timeout, 1), 120000);
    }

    private static String bodyToString(Object body) {
        if (body == null) {
            return null;
        }
        return (body instanceof String) ? (String) body : JSON.serialize(body);
    }

    private static String abbreviate(String value, Integer maxLen) {
        if (value == null) {
            return null;
        }
        return value.length() <= maxLen ? value : value.abbreviate(maxLen);
    }
}
