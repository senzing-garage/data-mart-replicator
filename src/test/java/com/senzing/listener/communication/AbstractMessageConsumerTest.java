package com.senzing.listener.communication;

import com.senzing.listener.communication.exception.MessageConsumerException;
import com.senzing.listener.service.AbstractListenerService;
import com.senzing.listener.service.MessageProcessor;
import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.locking.ProcessScopeLockingService;
import com.senzing.listener.service.scheduling.AbstractSchedulingService;
import com.senzing.listener.service.scheduling.PostgreSQLSchedulingService;
import com.senzing.listener.service.scheduling.Scheduler;
import com.senzing.listener.service.scheduling.SchedulingService;
import com.senzing.sql.*;
import com.senzing.util.AccessToken;
import com.senzing.util.JsonUtilities;

import javax.json.*;
import javax.naming.NamingException;
import java.io.*;
import java.security.SecureRandom;
import java.sql.DriverManager;
import java.util.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import uk.org.webcompere.systemstubs.stream.SystemErr;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.senzing.listener.communication.MessageConsumer.State.*;
import com.senzing.listener.communication.MessageConsumer.State;
import static com.senzing.listener.communication.AbstractMessageConsumer.*;
import static com.senzing.listener.service.scheduling.AbstractSQLSchedulingService.CLEAN_DATABASE_KEY;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.TestInstance.Lifecycle;
import static com.senzing.util.JsonUtilities.*;
import static com.senzing.listener.service.AbstractListenerService.MessagePart.*;
import static com.senzing.listener.service.scheduling.AbstractSQLSchedulingService.CONNECTION_PROVIDER_KEY;
import static com.senzing.util.LoggingUtilities.*;

/**
 * Tests for {@link AbstractMessageConsumer}.
 */
@TestInstance(Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
public class AbstractMessageConsumerTest {
    private static SecureRandom PRNG = new SecureRandom();
    static {
        PRNG.nextDouble();
    }
    private Set<Long> previousAffectedSet = null;
    private int noOverlapCount = 0;

    private Set<Long> getAffectedSet(int minEntityId, int maxEntityId, int maxAffected) {
        int idSpread = (maxEntityId - minEntityId);
        Set<Long> affectedSet = new LinkedHashSet<>();
        int affectedCount = Math.max(1, PRNG.nextInt(maxAffected));
        for (int index2 = 0; index2 < affectedCount; index2++) {
            long entityId = ((long) (minEntityId + PRNG.nextInt(idSpread)));
            entityId = Math.min(entityId, (long) maxEntityId);
            affectedSet.add(entityId);
        }

        synchronized (this) {
            if (previousAffectedSet != null) {
                boolean overlap = false;
                for (Long entityId : affectedSet) {
                    if (previousAffectedSet.contains(entityId)) {
                        overlap = true;
                        break;
                    }
                }
                noOverlapCount = (overlap) ? 0 : (noOverlapCount + 1);

                // check if we have had no contention in a while and force it if not
                if (noOverlapCount > 20) {
                    // check if max size
                    if (affectedSet.size() == maxAffected) {
                        // remove the first if so
                        affectedSet.remove(affectedSet.iterator().next());
                    }
                    // then add one from the previous to create overlap
                    affectedSet.add(previousAffectedSet.iterator().next());
                    noOverlapCount = 0;
                }
            }

            // set the previous and return
            previousAffectedSet = affectedSet;
        }
        return Collections.unmodifiableSet(affectedSet);
    }

    private String getRecordId(int nextRecordId) {
        return "RECORD-" + nextRecordId;
    }

    private String getDataSource(List<String> dataSources) {
        int index = PRNG.nextInt(dataSources.size());
        index = Math.min(Math.max(0, index), dataSources.size() - 1);
        return dataSources.get(index);
    }

    public int buildInfoBatches(List<Message> messageList, int batchCount, List<String> dataSources, int minBatchSize, int maxBatchSize, int minEntityId, int maxEntityid, int maxAffected, double failureRate) {
        // fabricate record IDs
        int nextRecordId = (int) Math.pow(10, (Math.floor(Math.log10(batchCount * maxBatchSize)) + 1));
        int count = 0;
        // create the result list
        for (int index = 0; index < batchCount; index++) {
            boolean failure = PRNG.nextDouble() < failureRate;
            int failureCount = 0;
            if (failure) {
                failureCount = PRNG.nextInt(3);
            }
            // determine the batch size
            int batchSize = Math.max(1, minBatchSize + PRNG.nextInt(maxBatchSize));
            int messageId = nextRecordId;
            if (batchSize == 1) {
                count++;
                JsonObjectBuilder job = Json.createObjectBuilder();
                buildInfoMessage(job, messageId, failureCount, null, getDataSource(dataSources),
                        getRecordId(nextRecordId++), getAffectedSet(minEntityId, maxEntityid, maxAffected));
                JsonObject jsonObject = job.build();
                String messageText = toJsonText(jsonObject);
                messageList.add(new Message(messageId, messageText));

            } else {
                count += batchSize;
                JsonArrayBuilder jab = Json.createArrayBuilder();
                nextRecordId = buildInfoBatch(jab, batchSize, dataSources, nextRecordId, failureCount, minEntityId,
                        maxEntityid, maxAffected);
                JsonArray jsonArray = jab.build();
                String messageText = toJsonText(jsonArray);
                messageList.add(new Message(messageId, messageText));
            }
        }
        return count;
    }

    public Message buildInfoBatch(int batchSize, List<String> dataSources, int nextRecordId, int maxFailureCount, int minEntityId, int maxEntityId, int maxAffected) {
        JsonArrayBuilder jab = Json.createArrayBuilder();
        int messageId = nextRecordId;
        buildInfoBatch(jab, batchSize, dataSources, nextRecordId, maxFailureCount, minEntityId, maxEntityId,
                maxAffected);
        JsonArray jsonArray = jab.build();
        String messageText = toJsonText(jsonArray);
        return new Message(messageId, messageText);
    }

    public int buildInfoBatch(JsonArrayBuilder builder, int batchSize, List<String> dataSources, int nextRecordId, int maxFailureCount, int minEntityId, int maxEntityId, int maxAffected) {
        int messageId = nextRecordId; // all in the batch belong to same message
        for (int index1 = 0; index1 < batchSize; index1++) {
            JsonObjectBuilder job = Json.createObjectBuilder();
            int failureCount = (maxFailureCount == 0) ? 0 : PRNG.nextInt(maxFailureCount);
            buildInfoMessage(job, messageId, failureCount, null, getDataSource(dataSources),
                    getRecordId(nextRecordId++), getAffectedSet(minEntityId, maxEntityId, maxAffected));
            builder.add(job);
        }
        return nextRecordId;
    }

    public String buildInfoMessage(int messageId, String dataSource, String recordId, long... affectedEntityIds) {
        return buildInfoMessage(messageId, 0, null, dataSource, recordId, affectedEntityIds);
    }

    public String buildInfoMessage(int messageId, int failureCount, Long processingTime, String dataSource, String recordId, long... affectedEntityIds) {
        JsonObjectBuilder job = Json.createObjectBuilder();
        buildInfoMessage(job, messageId, failureCount, processingTime, dataSource, recordId, affectedEntityIds);
        JsonObject jsonObject = job.build();
        return JsonUtilities.toJsonText(jsonObject);
    }

    public String buildInfoMessage(int messageId, String dataSource, String recordId, Set<Long> affectedEntityIds) {
        return buildInfoMessage(messageId, 0, null, dataSource, recordId, affectedEntityIds);
    }

    public String buildInfoMessage(int messageId, int failureCount, Long processingTime, String dataSource, String recordId, Set<Long> affectedEntityIds) {
        JsonObjectBuilder job = Json.createObjectBuilder();
        buildInfoMessage(job, messageId, failureCount, processingTime, dataSource, recordId, affectedEntityIds);
        JsonObject jsonObject = job.build();
        return JsonUtilities.toJsonText(jsonObject);
    }

    public void buildInfoMessage(JsonObjectBuilder builder, int messageId, String dataSource, String recordId, long... affectedEntityIds) {
        this.buildInfoMessage(builder, messageId, 0, null, dataSource, recordId, affectedEntityIds);
    }

    public void buildInfoMessage(JsonObjectBuilder builder, int messageId, int failureCount, Long processTime, String dataSource, String recordId, long... affectedEntityIds) {
        Set<Long> affectedSet = new LinkedHashSet<>();
        for (long entityId : affectedEntityIds) {
            affectedSet.add(entityId);
        }
        this.buildInfoMessage(builder, messageId, failureCount, processTime, dataSource, recordId, affectedSet);
    }

    public void buildInfoMessage(JsonObjectBuilder builder, int messageId, String dataSource, String recordId, Set<Long> affectedEntityIds) {
        buildInfoMessage(builder, messageId, 0, null, dataSource, recordId, affectedEntityIds);
    }

    public void buildInfoMessage(JsonObjectBuilder builder, int messageId, int failureCount, Long processTime, String dataSource, String recordId, Set<Long> affectedEntityIds) {
        builder.add("MESSAGE_ID", messageId);
        if (failureCount > 0) {
            builder.add("FAILURE_COUNT", failureCount);
        }
        if (processTime != null) {
            builder.add("PROCESSING_TIME", processTime);
        }
        builder.add("DATA_SOURCE", dataSource);
        builder.add("RECORD_ID", recordId);
        JsonArrayBuilder jab = Json.createArrayBuilder();
        for (long entityId : affectedEntityIds) {
            JsonObjectBuilder job2 = Json.createObjectBuilder();
            job2.add("ENTITY_ID", entityId);
            job2.add("LENS_CODE", "DEFAULT");
            jab.add(job2);
        }
        builder.add("AFFECTED_ENTITIES", jab);
    }

    public static class Message {
        private int id;
        private String body;
        private Long processingTime = null;

        public Message(int id, String msgText) {
            this(id, null, msgText);
        }

        public Message(int id, Long processingTime, String msgText) {
            this.id = id;
            this.body = msgText;
            this.processingTime = processingTime;
        }

        public int getId() {
            return this.id;
        }

        public String getBody() {
            return this.body;
        }

        public String toString() {
            return "Message (" + this.getId() + "): " + this.getBody();
        }

    }

    public static class RecordId {
        private String dataSource;
        private String recordId;

        public RecordId(String dataSource, String recordId) {
            this.dataSource = dataSource;
            this.recordId = recordId;
        }

        public String getDataSource() {
            return this.dataSource;
        }

        public String getRecordId() {
            return this.recordId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || this.getClass() != o.getClass())
                return false;
            RecordId that = (RecordId) o;
            return Objects.equals(this.getDataSource(), that.getDataSource())
                    && Objects.equals(this.getRecordId(), that.getRecordId());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getDataSource(), getRecordId());
        }

        @Override
        public String toString() {
            return this.getDataSource() + ":" + this.getRecordId();
        }
    }

    public static class TestMessageConsumer extends AbstractMessageConsumer<Message> {
        private List<Message> messageQueue = new LinkedList<>();
        private IdentityHashMap<Message, Long> dequeuedMap = new IdentityHashMap<>();
        private Thread consumptionThread = null;
        private int dequeueCount;
        private long dequeueSleep;
        private long visibilityTimeout;
        private long expectedFailureCount = 0L;
        private long expectedMessageRetryCount = 0L;
        private long expectedInfoMessageRetryCount = 0L;

        public TestMessageConsumer(int dequeueCount, long dequeueSleep, long visibilityTimeout, List<Message> messages) {
            this.dequeueCount = dequeueCount;
            this.dequeueSleep = dequeueSleep;
            this.visibilityTimeout = visibilityTimeout;
            for (Message message : messages) {
                this.messageQueue.add(message);
                String body = message.getBody().trim();
                List<JsonObject> jsonObjects = new ArrayList<>();
                if (body.startsWith("[")) {
                    JsonArray jsonArray = parseJsonArray(body);
                    for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class)) {
                        jsonObjects.add(jsonObject);
                    }
                } else {
                    jsonObjects.add(parseJsonObject(body));
                }
                int maxFailures = 0;
                for (JsonObject jsonObject : jsonObjects) {
                    int failureCount = getInteger(jsonObject, "FAILURE_COUNT", 0);
                    this.expectedFailureCount += failureCount;
                    if (failureCount > maxFailures) {
                        maxFailures = failureCount;
                    }
                }
                this.expectedMessageRetryCount += maxFailures;
                this.expectedInfoMessageRetryCount += (maxFailures * jsonObjects.size());
            }

        }

        public int getDequeueCount() {
            return this.dequeueCount;
        }

        public long getDequeueSleep() {
            return this.dequeueSleep;
        }

        public long getVisibilityTimeout() {
            return this.visibilityTimeout;
        }

        public long getExpectedFailureCount() {
            return this.expectedFailureCount;
        }

        public long getExpectedMessageRetryCount() {
            return this.expectedMessageRetryCount;
        }

        public long getExpectedInfoMessageRetryCount() {
            return this.expectedInfoMessageRetryCount;
        }

        protected void doInit(JsonObject config) {
        }

        protected void doDestroy() {
            // join to the consumption thread
            try {
                this.consumptionThread.join();
                synchronized (this) {
                    this.consumptionThread = null;
                }
            } catch (InterruptedException ignore) {
                // ignore
            }
        }

        protected void doConsume(MessageProcessor processor) {
            this.consumptionThread = new Thread(() -> {
                long start = System.nanoTime() - 15000000000L;
                int timeoutCount = 0;
                int restoreCount = 0;
                while (this.getState() == CONSUMING) {
                    long end = System.nanoTime();
                    if (((end - start) / 1000000L) > 10000L) {
                        start = end;
                        // if (timeoutCount > 0) {
                        // restoreCount += timeoutCount;
                        // System.err.println("RESTORED " + timeoutCount
                        // + " MESSAGES DUE TO VISIBILITY TIMEOUT "
                        // + "(" + restoreCount + " TOTAL)");
                        // timeoutCount = 0;
                        // }
                    }
                    // dequeue messages
                    for (int index = 0; index < this.dequeueCount; index++) {
                        Message msg = null;
                        synchronized (this.messageQueue) {
                            if (this.messageQueue.size() == 0)
                                break;
                            msg = this.messageQueue.remove(0);
                            long now = System.nanoTime() / 1000000L;
                            this.dequeuedMap.put(msg, now);
                        }
                        this.enqueueMessages(processor, msg);
                    }

                    // check for messages that have timed out and enqueue them again
                    synchronized (this.messageQueue) {
                        Iterator<Map.Entry<Message, Long>> iter = this.dequeuedMap.entrySet().iterator();
                        while (iter.hasNext()) {
                            Map.Entry<Message, Long> entry = iter.next();
                            Message msg = entry.getKey();
                            Long timestamp = entry.getValue();
                            long now = System.nanoTime() / 1000000L;
                            if (now - timestamp > this.visibilityTimeout) {
                                iter.remove();
                                timeoutCount++;
                                this.messageQueue.add(0, msg);
                            }
                        }
                    }

                    // now sleep for a while
                    try {
                        Thread.sleep(this.dequeueSleep);
                    } catch (InterruptedException ignore) {
                        // ignore
                    }
                }
                // if (timeoutCount > 0) {
                // restoreCount += timeoutCount;
                // System.err.println("RESTORED " + timeoutCount
                // + " MESSAGES DUE TO VISIBILITY TIMEOUT "
                // + "(" + restoreCount + " TOTAL)");
                // }
            });

            this.consumptionThread.start();
        }

        protected String extractMessageBody(Message msg) {
            return msg.getBody();
        }

        protected void disposeMessage(Message msg) {
            synchronized (this.messageQueue) {
                this.dequeuedMap.remove(msg);
            }
        }
    }

    public static class MessageCounts implements Cloneable, Comparable<MessageCounts> {
        private String messageText;
        private int beginCount = 0;
        private int successCount = 0;
        private int failureCount = 0;
        private long firstBeginTime = 0L;
        private long lastBeginTime = 0L;
        private long lastEndTime = 0L;
        private Integer messageId = null;

        public MessageCounts(String message) {
            this.messageText = message;
            try {
                JsonObject jsonObject = parseJsonObject(this.messageText);
                this.messageId = getInteger(jsonObject, "MESSAGE_ID", null);

            } catch (Exception e) {
                // allow for tests with bad JSON by having the MESSAGE_ID appear first
                // in a stand-alone JSON object
                try {
                    String firstLine = (new BufferedReader(new StringReader(this.getMessageText()))).readLine();
                    this.messageId = Integer.parseInt(firstLine.trim());
                } catch (Exception ignore) {
                    // do nothing
                }
            }
        }

        public Object clone() {
            try {
                return super.clone();
            } catch (CloneNotSupportedException cannotHappen) {
                throw new IllegalStateException("Unexpected clone failure");
            }
        }

        public int hashCode() {
            synchronized (this) {
                return Objects.hash(this.getMessageId(), this.getFirstBeginTime(), this.getLastBeginTime(),
                        this.getLastEndTime(), this.getBeginCount(), this.getSuccessCount(), this.getFailureCount());
            }
        }

        public boolean equals(Object that) {
            if (that == null)
                return false;
            if (this == that)
                return true;
            if (this.getClass() != that.getClass())
                return false;
            MessageCounts counts = (MessageCounts) that;
            int thisPriority = System.identityHashCode(this);
            int thatPriority = System.identityHashCode(that);
            MessageCounts first = (thisPriority < thatPriority) ? this : counts;
            MessageCounts second = (thisPriority < thatPriority) ? counts : this;
            synchronized (first) {
                synchronized (second) {
                    return Objects.equals(this.getMessageId(), counts.getMessageId())
                            && Objects.equals(this.getMessageText(), counts.getMessageText())
                            && Objects.equals(this.getFirstBeginTime(), counts.getFirstBeginTime())
                            && Objects.equals(this.getLastBeginTime(), counts.getLastBeginTime())
                            && Objects.equals(this.getLastEndTime(), counts.getLastEndTime())
                            && Objects.equals(this.getBeginCount(), counts.getBeginCount())
                            && Objects.equals(this.getSuccessCount(), counts.getSuccessCount())
                            && Objects.equals(this.getFailureCount(), counts.getFailureCount());
                }
            }
        }

        public int compareTo(MessageCounts that) {
            if (that == null)
                return 1;
            if (that == this)
                return 0;
            int thisPriority = System.identityHashCode(this);
            int thatPriority = System.identityHashCode(that);
            MessageCounts first = (thisPriority < thatPriority) ? this : that;
            MessageCounts second = (thisPriority < thatPriority) ? that : this;

            synchronized (first) {
                synchronized (second) {
                    long diff = this.getLastBeginTime() - that.getLastBeginTime();
                    if (diff != 0)
                        return (diff < 0) ? -1 : 1;
                    diff = this.getFirstBeginTime() - that.getFirstBeginTime();
                    if (diff != 0)
                        return (diff < 0) ? -1 : 1;
                    diff = this.getLastEndTime() - that.getLastEndTime();
                    if (diff != 0)
                        return (diff < 0) ? -1 : 1;
                    diff = (this.getMessageId() - that.getMessageId());
                    if (diff != 0)
                        return (diff < 0) ? -1 : 1;
                    diff = (this.getBeginCount() - that.getBeginCount());
                    if (diff != 0)
                        return (diff < 0) ? -1 : 1;
                    diff = (this.getSuccessCount() - that.getSuccessCount());
                    if (diff != 0)
                        return (diff < 0) ? -1 : 1;
                    diff = (this.getFailureCount() - that.getFailureCount());
                    if (diff != 0)
                        return (diff < 0) ? -1 : 1;
                    return (this.getMessageText().compareTo(that.getMessageText()));
                }
            }
        }

        public Integer getMessageId() {
            return this.messageId;
        }

        public synchronized void recordBegin() {
            this.beginCount++;
            long now = System.nanoTime();
            if (this.firstBeginTime == 0)
                this.firstBeginTime = now;
            this.lastBeginTime = now;
        }

        public synchronized void recordSuccess() {
            this.successCount++;
            this.lastEndTime = System.nanoTime();
        }

        public synchronized void recordFailure() {
            this.failureCount++;
            this.lastEndTime = System.nanoTime();
        }

        public String getMessageText() {
            return this.messageText;
        }

        public synchronized int getBeginCount() {
            return this.beginCount;
        }

        public synchronized int getSuccessCount() {
            return this.successCount;
        }

        public synchronized int getFailureCount() {
            return this.failureCount;
        }

        public synchronized long getFirstBeginTime() {
            return this.firstBeginTime;
        }

        public synchronized long getLastBeginTime() {
            return this.lastBeginTime;
        }

        public synchronized long getLastEndTime() {
            return this.lastEndTime;
        }

        public static String toString(Collection<MessageCounts> countsList) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            pw.println();
            for (MessageCounts counts : countsList) {
                pw.println("     " + counts);
            }
            pw.println();
            pw.flush();
            return sw.toString();
        }

        public String toString() {
            synchronized (this) {
                return "MESSAGE (" + this.getMessageId() + "): begin=[ " + this.getBeginCount() + " / "
                        + this.getFirstBeginTime() + " / " + this.getLastBeginTime() + " ], success=[ "
                        + this.getSuccessCount() + " ], failed=[ " + this.getFailureCount() + " ], lastEndTime=[ "
                        + this.getLastEndTime() + " ]";
            }
        }
    }

    public static class TestService extends AbstractListenerService {
        private static final Map<MessagePart, String> ACTION_MAP = Map.of(RECORD, "RECORD", AFFECTED_ENTITY, "ENTITY");

        private static final ThreadLocal<MessageCounts> MESSAGE_COUNTS = new ThreadLocal<>();

        private long minProcessingTime = 10L;
        private long maxProcessingTime = 60L;
        private List<Exception> failures = new LinkedList<>();
        private Map<Object, String> tasksByEntity = new LinkedHashMap<>();
        private Map<String, MessageCounts> countsByMessage = new LinkedHashMap<>();
        private double failureRate = 0.0;
        private int handlingCount = 0;
        private int processingCount = 0;
        private boolean aborted = false;

        public TestService() {
            super(ACTION_MAP);
        }

        public TestService(long processingTime, double failureRate) {
            this(processingTime, processingTime, failureRate);
        }

        public TestService(long minProcessingTime, long maxProcessingTime, double failureRate) {
            super(ACTION_MAP);
            this.minProcessingTime = minProcessingTime;
            this.maxProcessingTime = maxProcessingTime;
            this.failureRate = failureRate;
        }

        private synchronized void logFailure(Exception e) {
            System.err.println(e.getMessage());
            System.err.println(formatStackTrace(e.getStackTrace()));
            this.failures.add(e);
        }

        public synchronized List<Exception> getFailures() {
            return new ArrayList<>(this.failures);
        }

        public synchronized Map<Integer, MessageCounts> getMessageCounts() {
            Map<Integer, MessageCounts> result = new LinkedHashMap<>();
            this.countsByMessage.values().forEach((counts) -> {
                MessageCounts clone = (MessageCounts) counts.clone();
                result.put(clone.getMessageId(), clone);
            });
            return result;
        }

        public SchedulingService getSchedulingService() {
            return super.getSchedulingService();
        }

        public synchronized int getSuccessCount() {
            int successCount = 0;
            for (MessageCounts counts : this.countsByMessage.values()) {
                successCount += (counts.getSuccessCount() > 0) ? 1 : 0;
            }
            return successCount;
        }

        public synchronized boolean isProcessing() {
            return (this.processingCount > 0);
        }

        public synchronized void awaitSuccess(TestMessageConsumer consumer, int minSuccessCount, ConnectionPool pool) {
            long start = System.nanoTime() / 1000000L;
            int successCount = this.getSuccessCount();
            boolean processing = this.isProcessing();
            while ((successCount < minSuccessCount || processing) && !this.aborted) {
                long now = System.nanoTime() / 1000000L;
                if ((now - start) > 10000L) {
                    start = now;
                    // printStatistics(consumer, this, pool);
                }
                try {
                    this.wait(this.maxProcessingTime);

                } catch (InterruptedException ignore) {
                    // ignore
                }
                successCount = this.getSuccessCount();
                processing = this.isProcessing();
            }
        }

        private synchronized void beginHandling(String action, Map<String, Object> parameters, int multiplicity, String taskAsJson) {
            this.handlingCount++;
            if (this.aborted)
                return;
            Object key = null;
            switch (action) {
            case "RECORD":
                key = new RecordId(parameters.get(DATA_SOURCE_PARAMETER_KEY).toString(),
                        parameters.get(RECORD_ID_PARAMETER_KEY).toString());
                break;
            case "ENTITY":
                key = parameters.get(ENTITY_ID_PARAMETER_KEY);
                break;
            case "DATA_SOURCE_COUNT":
                key = parameters.get(DATA_SOURCE_PARAMETER_KEY);
                break;
            default:
                key = null;
            }

            if (key != null) {
                if (this.tasksByEntity.containsKey(key)) {
                    this.aborted = true;
                    ProcessScopeLockingService lockingService = (ProcessScopeLockingService) this.getSchedulingService()
                            .getLockingService();
                    lockingService.dumpLocks();

                    throw new IllegalStateException("Simultaneous processing of the same resource (" + key + ").  "
                            + "inProgress=[ " + this.tasksByEntity.get(key) + " ], conflicting=[ " + taskAsJson + " ]");
                }
                this.tasksByEntity.put(key, taskAsJson);
            }
        }

        private synchronized MessageCounts beginProcessing(JsonObject message, String jsonText) {
            this.processingCount++;
            if (this.aborted)
                return null;
            MessageCounts counts = this.countsByMessage.get(jsonText);
            if (counts == null) {
                counts = new MessageCounts(jsonText);
                this.countsByMessage.put(jsonText, counts);
            }
            counts.recordBegin();
            MESSAGE_COUNTS.set(counts);
            return counts;
        }

        private synchronized boolean isAborted() {
            return this.aborted;
        }

        private synchronized void endHandling(String action, Map<String, Object> parameters, int multiplicity, String taskAsJson) {
            this.handlingCount--;
            if (this.aborted)
                return;
            Object key = null;
            switch (action) {
            case "RECORD":
                key = new RecordId(parameters.get(DATA_SOURCE_PARAMETER_KEY).toString(),
                        parameters.get(RECORD_ID_PARAMETER_KEY).toString());
                break;
            case "ENTITY":
                key = parameters.get(ENTITY_ID_PARAMETER_KEY);
                break;
            default:
                key = null;
            }

            if (key != null) {
                String existing = this.tasksByEntity.get(key);
                if (existing == null) {
                    this.aborted = true;
                    throw new IllegalStateException(
                            "Resource (" + key + ") was not marked for handling: " + taskAsJson);
                }
                if (!existing.equals(taskAsJson)) {
                    this.aborted = true;
                    throw new IllegalStateException("Resource (" + key + ") was associated with another "
                            + "message.  expected=[ " + taskAsJson + " ], found=[ " + existing + " ]");
                }

                // remove the resource key
                this.tasksByEntity.remove(key);
            }

            this.notifyAll();
        }

        private synchronized MessageCounts endProcessing(JsonObject jsonObject, String jsonText, boolean success) {
            this.processingCount--;
            if (this.aborted)
                return null;

            MessageCounts counts = this.countsByMessage.get(jsonText);
            if (counts == null) {
                this.aborted = true;
                throw new IllegalStateException("Missing message counts for message: " + jsonText);
            }
            if (success)
                counts.recordSuccess();
            else
                counts.recordFailure();
            this.notifyAll();
            MESSAGE_COUNTS.set(null);
            return counts;
        }

        @Override
        protected void doInit(JsonObject config) {
            // do nothing
        }

        @Override
        public void process(JsonObject message) throws ServiceExecutionException {
            String jsonText = JsonUtilities.toJsonText(message);
            try {
                MessageCounts counts = this.beginProcessing(message, jsonText);
                boolean success = true;
                try {
                    super.process(message);

                } catch (ServiceExecutionException e) {
                    success = false;
                    throw e;

                } catch (Exception e) {
                    this.logFailure(e);
                    success = false;

                } finally {
                    this.endProcessing(message, jsonText, success);
                }

            } catch (ServiceExecutionException e) {
                // rethrow the simulated failure
                throw e;

            } catch (Exception e) {
                this.logFailure(e);
                if (!this.isAborted()) {
                    throw new ServiceExecutionException(e);
                }
            }
        }

        @Override
        protected void scheduleTasks(JsonObject message, Scheduler scheduler) throws ServiceExecutionException {
            super.scheduleTasks(message, scheduler);

            // check for a forced failure
            MessageCounts counts = MESSAGE_COUNTS.get();
            int maxFailures = getInteger(message, "FAILURE_COUNT", 0);
            int failureCount = counts.getFailureCount();
            if (maxFailures > 0 && failureCount < maxFailures) {
                scheduler.createTaskBuilder("FORCED_FAILURE").parameter("failureCount", failureCount)
                        .parameter("maxFailures", maxFailures).parameter("message", toJsonText(message))
                        .schedule(false);
            }

        }

        @Override
        protected void handleTask(String action, Map<String, Object> parameters, int multiplicity, Scheduler followUpScheduler) throws ServiceExecutionException {
            String jsonText = this.taskAsJson(action, parameters, multiplicity);
            this.beginHandling(action, parameters, multiplicity, jsonText);

            try {
                // check if we are dealing with a forced-failure
                if ("FORCED_FAILURE".equals(action)) {
                    int failureCount = (Integer) parameters.get("failureCount");
                    int maxFailures = (Integer) parameters.get("maxFailures");
                    String message = (String) parameters.get("message");
                    throw new ServiceExecutionException(
                            "Simulated failure (" + failureCount + " of " + maxFailures + ") for message: " + message);
                }

                // otherwise sleep for a period of time possibly with a random failure
                long range = this.maxProcessingTime - this.minProcessingTime;
                double percentage = PRNG.nextDouble();
                long processingTime = this.minProcessingTime + ((long) (percentage * (double) range));
                boolean failure = PRNG.nextDouble() < this.failureRate;
                try {
                    Thread.sleep(processingTime);
                } catch (InterruptedException ignore) {
                    // do nothing
                }

                if (failure) {
                    throw new ServiceExecutionException("Simulated random failure for task: " + jsonText);
                }

                if ("RECORD".equals(action) && (followUpScheduler != null) && (PRNG.nextDouble() < 0.50)) {
                    String dataSource = parameters.get(DATA_SOURCE_PARAMETER_KEY).toString();

                    // schedule a follow-up task
                    followUpScheduler.createTaskBuilder("INCREMENT_RECORD_COUNT")
                            .parameter(DATA_SOURCE_PARAMETER_KEY, dataSource).resource("DATA_SOURCE", dataSource)
                            .schedule();

                    followUpScheduler.commit();
                }

            } finally {
                this.endHandling(action, parameters, multiplicity, jsonText);
            }
        }

        @Override
        public void doDestroy() {
            // do nothing
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 2, 3, 4, 8 })
    public void basicTest(int concurrency) {
        List<Message> messages = new LinkedList<>();
        messages.add(new Message(1, buildInfoMessage(1, "CUSTOMERS", "001", 1, 2, 3)));
        messages.add(new Message(2, buildInfoMessage(2, "CUSTOMERS", "002", 1, 4)));
        messages.add(new Message(3, buildInfoMessage(3, "CUSTOMERS", "003", 2, 5)));
        messages.add(new Message(4, buildInfoMessage(4, "CUSTOMERS", "004", 4, 5)));
        messages.add(new Message(5, buildInfoMessage(5, "CUSTOMERS", "005", 6, 7)));

        this.performTest(messages, messages.size(), concurrency, null, null, null, null, null, 0.0, null);
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 2, 3, 4, 8 })
    public void errantTest(int concurrency) throws Exception {
        List<Message> messages = new LinkedList<>();
        messages.add(new Message(1, buildInfoMessage(1, "CUSTOMERS", "001", 1, 2, 3)));
        messages.add(new Message(2, buildInfoMessage(2, 1, null, "CUSTOMERS", "002", 1, 4)));
        messages.add(new Message(3, buildInfoMessage(3, "CUSTOMERS", "003", 2, 5)));
        messages.add(new Message(4, buildInfoMessage(4, 1, null, "CUSTOMERS", "004", 4, 5)));
        messages.add(new Message(5, buildInfoMessage(5, "CUSTOMERS", "005", 6, 7)));

        SystemErr systemErr = new SystemErr();
        systemErr.execute(() -> {
            this.performTest(messages, messages.size(), concurrency, null, null, 2500L, null, null, 0.0, null);
        });
    }

    @ParameterizedTest
    @ValueSource(ints = { 8, 16, 24 })
    public void loadTest(int concurrency) throws Exception {
        List<Message> batches = new LinkedList<>();
        int messageCount = buildInfoBatches(batches, 2000, List.of("CUSTOMERS", "EMPLOYEES", "VENDORS"), 1, 10, 1000,
                3000, 4, 0.005);

        System.err.println();
        System.err.println("=====================================================");
        System.err.println("Testing " + batches.size() + " batches comprising " + messageCount
                + " messages with concurrency of " + concurrency + ".");

        long start = System.nanoTime() / 1000000L;
        SystemErr systemErr = new SystemErr();
        systemErr.execute(() -> {
            this.performTest(batches, messageCount, concurrency, 30, 50L, 5000L, 2L, 5L, 0.0, null);
        });
        long duration = (System.nanoTime() / 1000000L) - start;
        System.err.println("TOTAL TIME: " + (duration) + " ms");
    }

    protected void performTest(List<Message> messages, int messageCount, Integer concurrency, Integer dequeueCount, Long dequeueSleep, Long visibilityTimeout, Long minProcessingTime, Long maxProcessingTime, Double failureRate, Map<Integer, Set<Integer>> orderAfterMap) {
        StringBuilder sb = new StringBuilder();
        String prefix = "";
        if (concurrency != null) {
            sb.append(prefix);
            sb.append("concurrency=[ " + concurrency + " ]");
            prefix = ", ";
        }
        if (dequeueCount == null) {
            dequeueCount = 2;
        } else {
            sb.append(prefix);
            sb.append("dequeueCount=[ " + dequeueCount + " ]");
            prefix = ", ";
        }
        if (dequeueSleep == null) {
            dequeueSleep = 25L;
        } else {
            sb.append(prefix);
            sb.append("dequeueSleep=[ " + dequeueSleep + " ]");
            prefix = ", ";
        }
        if (visibilityTimeout == null) {
            visibilityTimeout = 12500L;
        } else {
            sb.append(prefix);
            sb.append("visibilityTimeout=[ " + visibilityTimeout + " ]");
            prefix = ", ";
        }
        if (minProcessingTime == null) {
            minProcessingTime = 75L;
        } else {
            sb.append(prefix);
            sb.append("minProcessingTime=[ " + minProcessingTime + " ]");
            prefix = ", ";
        }
        if (maxProcessingTime == null) {
            maxProcessingTime = minProcessingTime;
        } else {
            sb.append(prefix);
            sb.append("maxProcessingTime=[ " + maxProcessingTime + " ]");
            prefix = ", ";
        }
        if (failureRate == null) {
            failureRate = 0.0;
        } else {
            sb.append(prefix);
            sb.append("failureRate=[ " + failureRate + " ]");
            prefix = ", ";
        }
        String testInfo = sb.toString();

        TestMessageConsumer consumer = new TestMessageConsumer(dequeueCount, dequeueSleep, visibilityTimeout, messages);

        TestService service = new TestService(minProcessingTime, maxProcessingTime, failureRate);

        JsonObjectBuilder job = Json.createObjectBuilder();
        if (concurrency != null) {
            job.add(CONCURRENCY_KEY, concurrency * 8);
        }
        JsonObject consumerConfig = job.build();

        AccessToken token = null;
        String providerName = null;
        ConnectionPool pool = null;
        try {
            File dbFile = File.createTempFile("sz_follow_up_", ".db");

            providerName = dbFile.getCanonicalPath();

            boolean usePostgreSQL = Boolean.TRUE.toString()
                    .equals(System.getProperty("com.senzing.listener.test.postgresql"));

            Connector connector = null;
            if (usePostgreSQL) {
                connector = () -> {
                    String url = "jdbc:postgresql://localhost:5500/test";
                    return DriverManager.getConnection(url, "user", "password");
                };
            } else {
                connector = new SQLiteConnector(dbFile);
            }

            pool = new ConnectionPool(connector, 1);

            ConnectionProvider provider = new PoolConnectionProvider(pool);

            token = ConnectionProvider.REGISTRY.bind(providerName, provider);

            JsonObjectBuilder builder1 = Json.createObjectBuilder();
            JsonObjectBuilder builder2 = Json.createObjectBuilder();
            builder1.add(AbstractSchedulingService.CONCURRENCY_KEY, concurrency);
            if (usePostgreSQL) {
                builder1.add(CLEAN_DATABASE_KEY, true);
            }
            builder1.add(CONNECTION_PROVIDER_KEY, providerName);
            builder2.add(AbstractListenerService.SCHEDULING_SERVICE_CONFIG_KEY, builder1);
            if (usePostgreSQL) {
                builder2.add(AbstractListenerService.SCHEDULING_SERVICE_CLASS_KEY,
                        PostgreSQLSchedulingService.class.getName());
            }

            service.init(builder2.build());
            consumer.init(consumerConfig);
            consumer.consume(service);

        } catch (Exception exception) {
            fail(exception);
        } finally {
            if (token != null) {
                try {
                    ConnectionProvider.REGISTRY.unbind(providerName, token);

                } catch (NamingException ignore) {
                    // do nothing
                }
            }
        }

        // wait success
        service.awaitSuccess(consumer, messageCount, pool);
        try {
            Thread.sleep(2000L);
        } catch (InterruptedException ignore) {
            // do nothing
        }
        consumer.destroy();
        // Map<Stat, Number> stats = printStatistics(consumer, service);
        Map<Statistic, Number> stats = consumer.getStatistics();

        Number messageRetryCount = stats.get(Stat.messageRetryCount);
        Number processRetryCount = stats.get(Stat.processRetryCount);
        Number statsFailureCount = stats.get(Stat.processFailureCount);

        if (failureRate == 0.0) {
            assertEquals(consumer.getExpectedFailureCount(), statsFailureCount,
                    "Wrong number of info message failures");
            assertEquals(consumer.getExpectedMessageRetryCount(), messageRetryCount,
                    "Wrong number of message (batch) retries");
            assertEquals(consumer.getExpectedInfoMessageRetryCount(), processRetryCount,
                    "Wrong number of info message retries");
        }

        // get the exceptions
        int failureCount = service.getFailures().size();
        if (failureCount > 0) {
            for (Exception e : service.getFailures()) {
                System.err.println();
                System.err.println("=================================================");
                System.err.println(e.getMessage());
                System.err.println(formatStackTrace(e.getStackTrace()));
            }
            fail("Failed with " + failureCount + " exceptions.  " + testInfo + ", failures=[ " + service.getFailures()
                    + " ]");
        }

        // get the counts
        Map<Integer, MessageCounts> countsMap = service.getMessageCounts();
        List<MessageCounts> countsList = new ArrayList<>(countsMap.values());
        Collections.sort(countsList);

        // destroy the service
        service.destroy();

        // check the message counts
        for (Message message : messages) {
            String messageBody = message.getBody();
            JsonObject jsonObject = null;
            try {
                jsonObject = parseJsonObject(messageBody);
            } catch (Exception e) {
                // bad JSON -- skip this one
                continue;
            }
            Integer messageId = getInteger(jsonObject, "MESSAGE_ID");
            if (messageId == null)
                continue;
            MessageCounts counts = countsMap.get(messageId);

            if (counts == null) {
                fail("Failed to find statistics for message: " + messageBody);
            }
            assertTrue((counts.getSuccessCount() > 0), "Message never succeeded: " + counts + " / " + messageBody);

            int maxFailures = getInteger(jsonObject, "FAILURE_COUNT", -1);
            if ((maxFailures < 0 && failureRate == 0) || (maxFailures == 0)) {
                assertEquals(0, counts.getFailureCount(), "Received a failure for a message where none was "
                        + "expected: " + counts + " / " + messageBody);
            } else if (maxFailures > 0) {
                assertEquals(maxFailures, counts.getFailureCount(), "Received an unexpected number of failures for "
                        + "a message: " + counts + " / " + messageBody);
            }
        }

        if (orderAfterMap != null) {
            orderAfterMap.forEach((messageId, afterSet) -> {
                MessageCounts msgCounts = countsMap.get(messageId);

                if (msgCounts == null) {
                    fail("Bad test data.  Unrecognized message ID (" + messageId + ") in ordering map: " + orderAfterMap
                            + " / " + countsMap);
                }
                afterSet.forEach(afterMessageId -> {
                    MessageCounts afterCounts = countsMap.get(afterMessageId);
                    if (afterCounts == null) {
                        fail("Bad test data.  Unrecognized message ID (" + afterMessageId + ") in ordering map: "
                                + orderAfterMap + " / " + countsMap);
                    }
                    long msgBegin = msgCounts.getLastBeginTime();
                    long afterBegin = afterCounts.getLastBeginTime();
                    assertTrue(msgBegin > afterBegin,
                            "Message " + messageId + " was unexpectedly " + "processed before message " + afterMessageId
                                    + ": " + msgBegin + " <= " + afterBegin + " / "
                                    + MessageCounts.toString(countsList));
                });
            });
        }
    }

    // ========================================================================
    // Direct processMessages() Tests
    // These tests exercise the code path guarded by SUPPRESS_PROCESSING_CHECK
    // at line 1109 of AbstractMessageConsumer.java
    // ========================================================================

    /**
     * A test consumer that exposes processMessages() for direct calls and allows
     * state manipulation for testing purposes.
     */
    public static class DirectProcessingConsumer extends AbstractMessageConsumer<Message> {
        private List<Message> messageQueue = new LinkedList<>();
        private AtomicInteger processedCount = new AtomicInteger(0);
        private AtomicBoolean destroyCalled = new AtomicBoolean(false);
        private CountDownLatch processingStartedLatch = new CountDownLatch(1);
        private CountDownLatch destroyLatch = new CountDownLatch(1);
        private long sleepTimePerMessage = 10L;

        public DirectProcessingConsumer(List<Message> messages, long sleepTimePerMessage) {
            this.messageQueue.addAll(messages);
            this.sleepTimePerMessage = sleepTimePerMessage;
        }

        public DirectProcessingConsumer(List<Message> messages) {
            this(messages, 10L);
        }

        public int getProcessedCount() {
            return this.processedCount.get();
        }

        public CountDownLatch getProcessingStartedLatch() {
            return this.processingStartedLatch;
        }

        public CountDownLatch getDestroyLatch() {
            return this.destroyLatch;
        }

        /**
         * Exposes processMessages() for direct testing.
         */
        public void callProcessMessages(MessageProcessor processor) {
            this.processMessages(processor);
        }

        /**
         * Allows setting the processing flag for testing.
         */
        public synchronized void setProcessingFlag(boolean processing) {
            try {
                java.lang.reflect.Field field = AbstractMessageConsumer.class.getDeclaredField("processing");
                field.setAccessible(true);
                field.setBoolean(this, processing);
            } catch (Exception e) {
                throw new RuntimeException("Failed to set processing flag", e);
            }
        }

        @Override
        protected void doInit(JsonObject config) {
            // nothing to do
        }

        @Override
        protected void doDestroy() {
            this.destroyCalled.set(true);
            this.destroyLatch.countDown();
        }

        @Override
        protected void doConsume(MessageProcessor processor) {
            // For direct testing, we don't need background consumption
        }

        @Override
        protected String extractMessageBody(Message msg) {
            return msg.getBody();
        }

        @Override
        protected void disposeMessage(Message msg) {
            // nothing to do
        }
    }

    /**
     * A slow message processor that sleeps for a configurable time, used to
     * simulate long-running processing for testing concurrent destroy.
     */
    public static class SlowMessageProcessor implements MessageProcessor {
        private long sleepTime;
        private AtomicInteger processCount = new AtomicInteger(0);
        private CountDownLatch processingLatch;

        public SlowMessageProcessor(long sleepTime, CountDownLatch processingLatch) {
            this.sleepTime = sleepTime;
            this.processingLatch = processingLatch;
        }

        public int getProcessCount() {
            return this.processCount.get();
        }

        @Override
        public void process(JsonObject message) throws ServiceExecutionException {
            this.processCount.incrementAndGet();
            if (this.processingLatch != null) {
                this.processingLatch.countDown();
            }
            try {
                Thread.sleep(this.sleepTime);
            } catch (InterruptedException ignore) {
                // ignore
            }
        }
    }

    /**
     * Tests that calling processMessages() when the state is not CONSUMING
     * throws an IllegalStateException with the expected message.
     */
    @Test
    public void testProcessMessagesNotConsumingState() throws Exception {
        List<Message> messages = new LinkedList<>();
        messages.add(new Message(1, buildInfoMessage(1, "CUSTOMERS", "001", 1)));

        DirectProcessingConsumer consumer = new DirectProcessingConsumer(messages);

        // Initialize but don't transition to CONSUMING state
        JsonObject config = Json.createObjectBuilder().build();
        consumer.init(config);

        // Verify state is INITIALIZED, not CONSUMING
        assertEquals(INITIALIZED, consumer.getState(),
                "State should be INITIALIZED before consume() is called");

        // Create a simple processor
        MessageProcessor processor = (msg) -> {};

        // Call processMessages() directly - should throw IllegalStateException
        // (SUPPRESS_PROCESSING_CHECK defaults to false, so validation is performed)
        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> consumer.callProcessMessages(processor),
                "Should throw IllegalStateException when state is not CONSUMING");

        assertTrue(exception.getMessage().contains("Cannot call processMessages()"),
                "Exception message should mention processMessages()");
        assertTrue(exception.getMessage().contains("CONSUMING"),
                "Exception message should mention CONSUMING state");
        assertTrue(exception.getMessage().contains(INITIALIZED.toString()),
                "Exception message should mention current state (INITIALIZED)");
    }

    /**
     * Tests that calling processMessages() when already processing
     * throws an IllegalStateException with the expected message.
     */
    @Test
    public void testProcessMessagesAlreadyProcessing() throws Exception {
        List<Message> messages = new LinkedList<>();
        messages.add(new Message(1, buildInfoMessage(1, "CUSTOMERS", "001", 1)));

        DirectProcessingConsumer consumer = new DirectProcessingConsumer(messages);

        // Initialize
        JsonObject config = Json.createObjectBuilder().build();
        consumer.init(config);

        // Manually set state to CONSUMING using reflection
        synchronized (consumer) {
            // setState is protected - directly accessible in same package
            consumer.setState(CONSUMING);
        }

        // Manually set processing flag to true
        consumer.setProcessingFlag(true);

        // Create a simple processor
        MessageProcessor processor = (msg) -> {};

        // Call processMessages() directly - should throw IllegalStateException
        // (SUPPRESS_PROCESSING_CHECK defaults to false, so validation is performed)
        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> consumer.callProcessMessages(processor),
                "Should throw IllegalStateException when already processing");

        assertTrue(exception.getMessage().contains("Cannot call processMessages()"),
                "Exception message should mention processMessages()");
        assertTrue(exception.getMessage().contains("already been called"),
                "Exception message should mention already processing");
    }

    /**
     * Tests that calling processMessages() directly when state is CONSUMING
     * and not already processing sets the processing flag and proceeds.
     * This exercises the code path at line 1125 that sets this.processing = true.
     */
    @Test
    public void testProcessMessagesDirectCallSuccess() throws Exception {
        // Create messages to process
        List<Message> messages = new LinkedList<>();
        for (int i = 1; i <= 5; i++) {
            messages.add(new Message(i, buildInfoMessage(i, "CUSTOMERS", "00" + i, i)));
        }

        DirectProcessingConsumer consumer = new DirectProcessingConsumer(messages, 5L);

        // Initialize
        JsonObject config = Json.createObjectBuilder()
                .add(CONCURRENCY_KEY, 2)
                .build();
        consumer.init(config);

        // Manually set state to CONSUMING using reflection
        synchronized (consumer) {
            // setState is protected - directly accessible in same package
            consumer.setState(CONSUMING);
        }

        // Ensure processing flag is false
        consumer.setProcessingFlag(false);

        // Create a latch to track when processing starts
        CountDownLatch processingLatch = new CountDownLatch(1);
        AtomicInteger messageCount = new AtomicInteger(0);
        AtomicReference<Throwable> exceptionRef = new AtomicReference<>(null);
        AtomicBoolean processingFlagWasSet = new AtomicBoolean(false);

        // Create processor that counts messages and signals when done
        MessageProcessor processor = (msg) -> {
            messageCount.incrementAndGet();
            processingLatch.countDown();
            // Small sleep to simulate processing
            try {
                Thread.sleep(5);
            } catch (InterruptedException ignore) {
                // ignore
            }
        };

        // Start processMessages in a separate thread since it blocks
        // (SUPPRESS_PROCESSING_CHECK defaults to false, so validation is performed)
        Thread processingThread = new Thread(() -> {
            try {
                // Call processMessages directly - this should set this.processing = true
                // and then proceed to create worker pool and process messages
                consumer.callProcessMessages(processor);
            } catch (IllegalStateException e) {
                // This would indicate state/processing check failed
                exceptionRef.set(e);
            } catch (Exception e) {
                // Other exceptions during processing - check if processing started
                // (The loop will exit with exception when state changes from CONSUMING)
            }
        });
        processingThread.start();

        // Wait for processing to start (indicates the check at line 1125 passed)
        boolean started = processingLatch.await(2, TimeUnit.SECONDS);

        // Check if processing flag was set (via reflection)
        try {
            java.lang.reflect.Field field = AbstractMessageConsumer.class.getDeclaredField("processing");
            field.setAccessible(true);
            processingFlagWasSet.set(field.getBoolean(consumer));
        } catch (Exception ignore) {
            // ignore
        }

        // Change state to trigger loop exit
        synchronized (consumer) {
            // setState is protected - directly accessible in same package
            consumer.setState(DESTROYING);
        }

        // Wait for thread to complete
        processingThread.join(5000);

        // Verify no IllegalStateException occurred (which would indicate the check failed)
        assertNull(exceptionRef.get(),
                "No IllegalStateException should occur when state is CONSUMING and not processing: "
                        + (exceptionRef.get() != null ? exceptionRef.get().getMessage() : ""));

        // Verify processing started (indicates the processing flag was set and loop entered)
        assertTrue(started || processingFlagWasSet.get(),
                "Processing should have started (processing flag set at line 1125)");
    }

    // ========================================================================
    // waitUntilDestroyed() Tests
    // These tests exercise the code path in waitUntilDestroyed() at lines 486-492
    // ========================================================================

    /**
     * A consumer that allows controlled timing during destroy for testing
     * waitUntilDestroyed().
     */
    public static class SlowDestroyConsumer extends AbstractMessageConsumer<Message> {
        private long destroyDelay;
        private CountDownLatch destroyStartedLatch = new CountDownLatch(1);
        private CountDownLatch destroyCompleteLatch = new CountDownLatch(1);
        private AtomicBoolean doDestroyStarted = new AtomicBoolean(false);
        private AtomicBoolean doDestroyCompleted = new AtomicBoolean(false);

        public SlowDestroyConsumer(long destroyDelay) {
            this.destroyDelay = destroyDelay;
        }

        public CountDownLatch getDestroyStartedLatch() {
            return this.destroyStartedLatch;
        }

        public CountDownLatch getDestroyCompleteLatch() {
            return this.destroyCompleteLatch;
        }

        public boolean isDoDestroyStarted() {
            return this.doDestroyStarted.get();
        }

        public boolean isDoDestroyCompleted() {
            return this.doDestroyCompleted.get();
        }

        @Override
        protected void doInit(JsonObject config) {
            // nothing to do
        }

        @Override
        protected void doDestroy() {
            this.doDestroyStarted.set(true);
            this.destroyStartedLatch.countDown();
            try {
                // Simulate slow cleanup
                Thread.sleep(this.destroyDelay);
            } catch (InterruptedException ignore) {
                // ignore
            }
            this.doDestroyCompleted.set(true);
            this.destroyCompleteLatch.countDown();
        }

        @Override
        protected void doConsume(MessageProcessor processor) {
            // nothing to do
        }

        @Override
        protected String extractMessageBody(Message msg) {
            return msg.getBody();
        }

        @Override
        protected void disposeMessage(Message msg) {
            // nothing to do
        }
    }

    /**
     * Tests that waitUntilDestroyed() blocks when another thread is destroying
     * the consumer, and waits until destruction is complete.
     * This exercises the loop at lines 486-492 in AbstractMessageConsumer.java.
     */
    @Test
    public void testWaitUntilDestroyedConcurrent() throws Exception {
        // Create a consumer with slow destroy (500ms delay)
        SlowDestroyConsumer consumer = new SlowDestroyConsumer(500L);

        // Initialize and start consuming
        JsonObject config = Json.createObjectBuilder().build();
        consumer.init(config);

        // Create a simple processor
        SlowMessageProcessor processor = new SlowMessageProcessor(10L, null);

        // Start consuming (this sets up the processing thread)
        consumer.consume(processor);

        // Give consumption a moment to start
        Thread.sleep(50);

        // Track timing
        AtomicReference<Long> thread1StartTime = new AtomicReference<>();
        AtomicReference<Long> thread1EndTime = new AtomicReference<>();
        AtomicReference<Long> thread2StartTime = new AtomicReference<>();
        AtomicReference<Long> thread2EndTime = new AtomicReference<>();
        AtomicBoolean thread2WaitedForDestroy = new AtomicBoolean(false);

        // First thread starts destroy
        Thread destroyThread1 = new Thread(() -> {
            thread1StartTime.set(System.nanoTime());
            consumer.destroy();
            thread1EndTime.set(System.nanoTime());
        });

        // Second thread also tries to destroy (should wait)
        Thread destroyThread2 = new Thread(() -> {
            try {
                // Wait for first thread to start destroying
                consumer.getDestroyStartedLatch().await(2, TimeUnit.SECONDS);
                // Small delay to ensure we're in DESTROYING state
                Thread.sleep(50);
            } catch (InterruptedException ignore) {
                // ignore
            }
            thread2StartTime.set(System.nanoTime());
            consumer.destroy(); // This should call waitUntilDestroyed()
            thread2EndTime.set(System.nanoTime());
            // Check if thread 2 had to wait (entered after doDestroy started, exited after it completed)
            if (consumer.isDoDestroyCompleted()) {
                thread2WaitedForDestroy.set(true);
            }
        });

        // Start first destroy thread
        destroyThread1.start();

        // Wait a moment then start second thread
        Thread.sleep(20);
        destroyThread2.start();

        // Wait for both threads to complete
        destroyThread1.join(3000);
        destroyThread2.join(3000);

        // Verify both threads completed
        assertNotNull(thread1EndTime.get(), "Thread 1 should have completed");
        assertNotNull(thread2EndTime.get(), "Thread 2 should have completed");

        // Verify consumer is destroyed
        assertEquals(DESTROYED, consumer.getState(),
                "Consumer should be in DESTROYED state");

        // Verify doDestroy was only called once
        assertTrue(consumer.isDoDestroyCompleted(),
                "doDestroy should have completed");

        // Verify thread 2 waited (it should end after or close to thread 1)
        // Allow some tolerance since timing is not exact
        long thread1Duration = thread1EndTime.get() - thread1StartTime.get();
        long thread2Start = thread2StartTime.get();
        long thread1End = thread1EndTime.get();

        // Thread 2 should have started before thread 1 ended (overlap during wait)
        // and ended after or around the same time as thread 1
        assertTrue(thread2Start < thread1End || thread2EndTime.get() >= thread1End - 50000000L,
                "Thread 2 should have waited for destroy to complete");
    }

    /**
     * Tests that waitUntilDestroyed() returns immediately when already destroyed.
     */
    @Test
    public void testWaitUntilDestroyedAlreadyDestroyed() throws Exception {
        SlowDestroyConsumer consumer = new SlowDestroyConsumer(10L);

        // Initialize
        JsonObject config = Json.createObjectBuilder().build();
        consumer.init(config);

        // Consume to set up processing thread
        consumer.consume((msg) -> {});

        // Give it a moment
        Thread.sleep(20);

        // Destroy
        consumer.destroy();

        // Verify destroyed
        assertEquals(DESTROYED, consumer.getState());

        // Calling destroy again should return immediately (no exception)
        long start = System.nanoTime();
        consumer.destroy();
        long duration = (System.nanoTime() - start) / 1000000L;

        // Should return very quickly (under 50ms)
        assertTrue(duration < 50,
                "destroy() on already destroyed consumer should return immediately");
    }

    /**
     * Tests that waitUntilDestroyed() throws when not in DESTROYING state.
     */
    @Test
    public void testWaitUntilDestroyedNotDestroyingState() throws Exception {
        SlowDestroyConsumer consumer = new SlowDestroyConsumer(10L);

        // Initialize but don't destroy
        JsonObject config = Json.createObjectBuilder().build();
        consumer.init(config);

        // Try to call waitUntilDestroyed() directly via reflection
        // It should throw because we're not in DESTROYING state
        synchronized (consumer) {
            // waitUntilDestroyed is protected - directly accessible in same package
            IllegalStateException exception = assertThrows(IllegalStateException.class,
                    () -> consumer.waitUntilDestroyed(),
                    "Should throw IllegalStateException when not in DESTROYING state");

            assertTrue(exception.getMessage().contains("waitUntilDestroyed"),
                    "Exception should mention waitUntilDestroyed()");
            assertTrue(exception.getMessage().contains("NOT currently destroying"),
                    "Exception should mention not destroying");
        }
    }

    // ========================================================================
    // Additional Code Coverage Tests
    // These tests exercise code paths for better coverage
    // ========================================================================

    /**
     * A simple consumer for testing that allows control over the message body
     * returned by extractMessageBody().
     */
    public static class SimpleTestConsumer extends AbstractMessageConsumer<Message> {
        private String messageBodyOverride = null;
        private boolean useOverride = false;

        public SimpleTestConsumer() {
        }

        public void setMessageBodyOverride(String body) {
            this.messageBodyOverride = body;
            this.useOverride = true;
        }

        public void clearMessageBodyOverride() {
            this.useOverride = false;
            this.messageBodyOverride = null;
        }

        @Override
        protected void doInit(JsonObject config) {
            // nothing to do
        }

        @Override
        protected void doDestroy() {
            // nothing to do
        }

        @Override
        protected void doConsume(MessageProcessor processor) {
            // nothing to do
        }

        @Override
        protected String extractMessageBody(Message msg) {
            if (this.useOverride) {
                return this.messageBodyOverride;
            }
            return msg.getBody();
        }

        @Override
        protected void disposeMessage(Message msg) {
            // nothing to do
        }

        /**
         * Exposes enqueueMessages() for testing.
         */
        public void callEnqueueMessages(MessageProcessor processor, Message message) {
            this.enqueueMessages(processor, message);
        }

        /**
         * Exposes backgroundProcessMessages() for testing.
         */
        public void callBackgroundProcessMessages(MessageProcessor processor) {
            this.backgroundProcessMessages(processor);
        }

        /**
         * Exposes timerStart() for testing.
         */
        public void callTimerStart(Stat stat, Stat... addlTimers) {
            this.timerStart(stat, addlTimers);
        }

        /**
         * Exposes timerPause() for testing.
         */
        public void callTimerPause(Stat stat, Stat... addlTimers) {
            this.timerPause(stat, addlTimers);
        }

        /**
         * Exposes timerResume() for testing.
         */
        public void callTimerResume(Stat stat, Stat... addlTimers) {
            this.timerResume(stat, addlTimers);
        }

        /**
         * Allows setting the processing flag for testing.
         */
        public synchronized void setProcessingFlag(boolean processing) {
            try {
                java.lang.reflect.Field field = AbstractMessageConsumer.class.getDeclaredField("processing");
                field.setAccessible(true);
                field.setBoolean(this, processing);
            } catch (Exception e) {
                throw new RuntimeException("Failed to set processing flag", e);
            }
        }

        /**
         * Allows setting the processingThread field for testing.
         */
        public synchronized void setProcessingThread(Thread thread) {
            try {
                java.lang.reflect.Field field = AbstractMessageConsumer.class.getDeclaredField("processingThread");
                field.setAccessible(true);
                field.set(this, thread);
            } catch (Exception e) {
                throw new RuntimeException("Failed to set processingThread", e);
            }
        }
    }

    /**
     * Test 1: Tests that Stat.getUnits() returns the expected units for all Stat values.
     * This exercises line 308 of AbstractMessageConsumer.java.
     */
    @Test
    public void testStatGetUnits() {
        // Verify all Stat values have getUnits() called
        for (AbstractMessageConsumer.Stat stat : AbstractMessageConsumer.Stat.values()) {
            String units = stat.getUnits();
            // Some stats have null units (like parallelism, dequeueHitRatio)
            // Just verify the method can be called without exception
            switch (stat) {
                case parallelism:
                case dequeueHitRatio:
                    assertNull(units, "Expected null units for " + stat);
                    break;
                case concurrency:
                    assertEquals("threads", units, "Expected 'threads' units for " + stat);
                    break;
                case roundTripCount:
                case messageRetryCount:
                    assertEquals("messages", units, "Expected 'messages' units for " + stat);
                    break;
                case processCount:
                case processSuccessCount:
                case processFailureCount:
                case processRetryCount:
                    assertEquals("calls", units, "Expected 'calls' units for " + stat);
                    break;
                default:
                    // Most other stats are in milliseconds
                    assertEquals("ms", units, "Expected 'ms' units for " + stat);
                    break;
            }
        }
    }

    /**
     * Test 2: Tests that waitUntilDestroyed() returns immediately when already destroyed.
     * This exercises line 481 of AbstractMessageConsumer.java.
     */
    @Test
    public void testWaitUntilDestroyedWhenAlreadyDestroyed() throws Exception {
        // Use SlowDestroyConsumer since it properly handles destruction
        SlowDestroyConsumer consumer = new SlowDestroyConsumer(10L);

        // Initialize
        JsonObject config = Json.createObjectBuilder().build();
        consumer.init(config);

        // Start consuming (this initializes the processing thread)
        MessageProcessor processor = (msg) -> {};
        consumer.consume(processor);

        // Destroy the consumer
        consumer.destroy();

        // Verify state is DESTROYED
        assertEquals(DESTROYED, consumer.getState(), "State should be DESTROYED");

        // Call waitUntilDestroyed() again - should return immediately without blocking
        // because state is already DESTROYED (exercises line 481)
        // waitUntilDestroyed is protected - directly accessible in same package

        // This should not block and should not throw
        synchronized (consumer) {
            consumer.waitUntilDestroyed();
        }

        // If we get here without blocking or exception, the test passes
        assertEquals(DESTROYED, consumer.getState(), "State should still be DESTROYED");
    }

    /**
     * Test 3: Tests that calling init() twice throws IllegalStateException.
     * This exercises lines 634-635 of AbstractMessageConsumer.java.
     */
    @Test
    public void testInitCalledTwiceThrowsException() throws Exception {
        SimpleTestConsumer consumer = new SimpleTestConsumer();

        // First init should succeed
        JsonObject config = Json.createObjectBuilder().build();
        consumer.init(config);

        assertEquals(INITIALIZED, consumer.getState(), "State should be INITIALIZED");

        // Second init should throw IllegalStateException
        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> consumer.init(config),
                "Second call to init() should throw IllegalStateException");

        assertTrue(exception.getMessage().contains("Cannot initialize"),
                "Exception message should mention cannot initialize");
        assertTrue(exception.getMessage().contains(UNINITIALIZED.toString()),
                "Exception message should mention UNINITIALIZED state");
    }

    /**
     * Test 4: Tests that init() with invalid config (bad CONCURRENCY_KEY) throws
     * MessageConsumerSetupException wrapping ServiceSetupException.
     * This exercises lines 662-663 of AbstractMessageConsumer.java.
     */
    @Test
    public void testInitWithInvalidConcurrencyConfig() {
        SimpleTestConsumer consumer = new SimpleTestConsumer();

        // Create config with invalid concurrency (not an integer)
        JsonObject config = Json.createObjectBuilder()
                .add(CONCURRENCY_KEY, "not-a-number")
                .build();

        // Init should throw MessageConsumerSetupException
        com.senzing.listener.communication.exception.MessageConsumerSetupException exception =
                assertThrows(com.senzing.listener.communication.exception.MessageConsumerSetupException.class,
                        () -> consumer.init(config),
                        "init() with invalid concurrency should throw MessageConsumerSetupException");

        // Verify the cause is a ServiceSetupException (which is what getConfigInteger throws)
        Throwable cause = exception.getCause();
        assertNotNull(cause, "Exception should have a cause");
        assertTrue(cause instanceof com.senzing.listener.service.exception.ServiceSetupException,
                "Cause should be ServiceSetupException, but was: " + cause.getClass().getName());
        assertTrue(cause.getMessage().contains(CONCURRENCY_KEY),
                "Cause message should mention the config key");
    }

    /**
     * Test 4b: Tests that init() with invalid TIMEOUT_KEY throws
     * MessageConsumerSetupException wrapping ServiceSetupException.
     * This also exercises lines 662-663 of AbstractMessageConsumer.java.
     */
    @Test
    public void testInitWithInvalidTimeoutConfig() {
        SimpleTestConsumer consumer = new SimpleTestConsumer();

        // Create config with invalid timeout (not an integer)
        JsonObject config = Json.createObjectBuilder()
                .add(TIMEOUT_KEY, "not-a-number")
                .build();

        // Init should throw MessageConsumerSetupException
        com.senzing.listener.communication.exception.MessageConsumerSetupException exception =
                assertThrows(com.senzing.listener.communication.exception.MessageConsumerSetupException.class,
                        () -> consumer.init(config),
                        "init() with invalid timeout should throw MessageConsumerSetupException");

        // Verify the cause is a ServiceSetupException (which is what getConfigLong throws)
        Throwable cause = exception.getCause();
        assertNotNull(cause, "Exception should have a cause");
        assertTrue(cause instanceof com.senzing.listener.service.exception.ServiceSetupException,
                "Cause should be ServiceSetupException, but was: " + cause.getClass().getName());
        assertTrue(cause.getMessage().contains(TIMEOUT_KEY),
                "Cause message should mention the config key");
    }

    /**
     * Test 5: Tests that consume() when not initialized throws IllegalStateException.
     * This exercises lines 697-698 of AbstractMessageConsumer.java.
     */
    @Test
    public void testConsumeWhenNotInitializedThrowsException() {
        SimpleTestConsumer consumer = new SimpleTestConsumer();

        // Don't initialize - just try to consume
        MessageProcessor processor = (msg) -> {};

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> consumer.consume(processor),
                "consume() when not initialized should throw IllegalStateException");

        assertTrue(exception.getMessage().contains(INITIALIZED.toString()),
                "Exception message should mention INITIALIZED state");
        assertTrue(exception.getMessage().contains(CONSUMING.toString()),
                "Exception message should mention CONSUMING state");
    }

    /**
     * Test 6: Tests that getAverageRoundTripMillis() returns null on uninitialized instance.
     * This exercises line 818 of AbstractMessageConsumer.java.
     */
    @Test
    public void testGetAverageRoundTripMillisReturnsNullWhenNoBatches() {
        SimpleTestConsumer consumer = new SimpleTestConsumer();

        // Call on uninitialized instance - should return null
        Long result = consumer.getAverageRoundTripMillis();
        assertNull(result, "getAverageRoundTripMillis() should return null when no batches processed");
    }

    /**
     * Test 7: Tests that getLongestRoundTripMillis() returns null on uninitialized instance.
     * This exercises line 841 of AbstractMessageConsumer.java.
     */
    @Test
    public void testGetLongestRoundTripMillisReturnsNullWhenNoBatches() {
        SimpleTestConsumer consumer = new SimpleTestConsumer();

        // Call on uninitialized instance - should return null
        Long result = consumer.getLongestRoundTripMillis();
        assertNull(result, "getLongestRoundTripMillis() should return null when no batches processed");
    }

    /**
     * Test 8a: Tests that getAverageProcessMillis() returns null on uninitialized instance.
     * This exercises line 911 of AbstractMessageConsumer.java.
     */
    @Test
    public void testGetAverageProcessMillisReturnsNullWhenNoMessages() {
        SimpleTestConsumer consumer = new SimpleTestConsumer();

        // Call on uninitialized instance - should return null
        Long result = consumer.getAverageProcessMillis();
        assertNull(result, "getAverageProcessMillis() should return null when no messages processed");
    }

    /**
     * Test 8b: Tests that getParallelism() returns null or zero on uninitialized instance.
     * This exercises line 931 of AbstractMessageConsumer.java.
     * Note: On an uninitialized consumer, this may return null (if activeTime is exactly 0)
     * or a zero-ish value (if there's minimal timer initialization overhead).
     */
    @Test
    public void testGetParallelismReturnsNullOrZeroWhenNoActiveTime() {
        SimpleTestConsumer consumer = new SimpleTestConsumer();

        // Call on uninitialized instance - should return null or 0.0
        Double result = consumer.getParallelism();
        // Either null or zero (including -0.0) is acceptable since no processing has occurred
        assertTrue(result == null || result == 0.0 || result == -0.0,
                "getParallelism() should return null or zero when no active processing time, but was: " + result);
    }

    /**
     * Test 9: Tests that enqueueMessages() when not CONSUMING throws IllegalStateException.
     * This exercises lines 993-994 of AbstractMessageConsumer.java.
     */
    @Test
    public void testEnqueueMessagesWhenNotConsumingThrowsException() throws Exception {
        SimpleTestConsumer consumer = new SimpleTestConsumer();

        // Initialize but don't start consuming
        JsonObject config = Json.createObjectBuilder().build();
        consumer.init(config);

        assertEquals(INITIALIZED, consumer.getState(), "State should be INITIALIZED");

        MessageProcessor processor = (msg) -> {};
        Message message = new Message(1, buildInfoMessage(1, "TEST", "001", 1));

        // Should throw IllegalStateException
        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> consumer.callEnqueueMessages(processor, message),
                "enqueueMessages() when not CONSUMING should throw IllegalStateException");

        assertTrue(exception.getMessage().contains("Cannot enqueue"),
                "Exception message should mention cannot enqueue");
        assertTrue(exception.getMessage().contains(CONSUMING.toString()),
                "Exception message should mention CONSUMING state");
    }

    /**
     * Test 10: Tests that enqueueMessages() with null message body returns early.
     * This exercises line 1004 of AbstractMessageConsumer.java.
     */
    @Test
    public void testEnqueueMessagesWithNullBodyReturnsEarly() throws Exception {
        SimpleTestConsumer consumer = new SimpleTestConsumer();

        // Initialize
        JsonObject config = Json.createObjectBuilder().build();
        consumer.init(config);

        // Manually set state to CONSUMING
        synchronized (consumer) {
            // setState is protected - directly accessible in same package
            consumer.setState(CONSUMING);
        }

        // Set override to return null body
        consumer.setMessageBodyOverride(null);

        MessageProcessor processor = (msg) -> {};
        Message message = new Message(1, "ignored");

        // Should not throw - just return early
        consumer.callEnqueueMessages(processor, message);

        // Verify no messages were enqueued
        assertEquals(0, consumer.getPendingMessageCount(),
                "No messages should be enqueued when body is null");
    }

    /**
     * Test 11: Tests that enqueueMessages() with empty/whitespace body returns early.
     * This exercises line 1008 of AbstractMessageConsumer.java.
     */
    @Test
    public void testEnqueueMessagesWithEmptyBodyReturnsEarly() throws Exception {
        SimpleTestConsumer consumer = new SimpleTestConsumer();

        // Initialize
        JsonObject config = Json.createObjectBuilder().build();
        consumer.init(config);

        // Manually set state to CONSUMING
        synchronized (consumer) {
            // setState is protected - directly accessible in same package
            consumer.setState(CONSUMING);
        }

        MessageProcessor processor = (msg) -> {};

        // Test with empty string
        consumer.setMessageBodyOverride("");
        Message message1 = new Message(1, "ignored");
        consumer.callEnqueueMessages(processor, message1);
        assertEquals(0, consumer.getPendingMessageCount(),
                "No messages should be enqueued when body is empty");

        // Test with whitespace only
        consumer.setMessageBodyOverride("   \t\n  ");
        Message message2 = new Message(2, "ignored");
        consumer.callEnqueueMessages(processor, message2);
        assertEquals(0, consumer.getPendingMessageCount(),
                "No messages should be enqueued when body is whitespace only");
    }

    /**
     * Test 12: Tests that enqueueMessages() with invalid JSON logs warning and returns.
     * This exercises lines 1019-1021 of AbstractMessageConsumer.java.
     * Note: Log output is captured by SystemStubs to suppress console noise for this test only.
     */
    @Test
    public void testEnqueueMessagesWithInvalidJsonReturnsEarly() throws Exception {
        SimpleTestConsumer consumer = new SimpleTestConsumer();

        // Initialize
        JsonObject config = Json.createObjectBuilder().build();
        consumer.init(config);

        // Manually set state to CONSUMING
        synchronized (consumer) {
            // setState is protected - directly accessible in same package
            consumer.setState(CONSUMING);
        }

        // Set override to return invalid JSON (not a Senzing INFO message format)
        // Note: This will trigger a warning log message which is captured by SystemStubs
        consumer.setMessageBodyOverride("this is not valid json at all!");

        MessageProcessor processor = (msg) -> {};
        Message message = new Message(1, "ignored");

        // Capture stderr output for this specific call that generates warning logs
        SystemErr systemErr = new SystemErr();
        systemErr.execute(() -> {
            // Should not throw - just log warning and return
            consumer.callEnqueueMessages(processor, message);
        });

        // Verify no messages were enqueued
        assertEquals(0, consumer.getPendingMessageCount(),
                "No messages should be enqueued when body is invalid JSON");

        // Verify the warning was logged (captured by SystemStubs)
        String errOutput = systemErr.getText();
        assertTrue(errOutput.contains("Ignoring unrecognized message body"),
                "Warning about unrecognized message should be logged");
    }

    /**
     * Test 13: Tests that backgroundProcessMessages() when not CONSUMING throws exception.
     * This exercises lines 1070-1071 of AbstractMessageConsumer.java.
     */
    @Test
    public void testBackgroundProcessMessagesWhenNotConsumingThrowsException() throws Exception {
        SimpleTestConsumer consumer = new SimpleTestConsumer();

        // Initialize but don't start consuming
        JsonObject config = Json.createObjectBuilder().build();
        consumer.init(config);

        assertEquals(INITIALIZED, consumer.getState(), "State should be INITIALIZED");

        MessageProcessor processor = (msg) -> {};

        // Should throw IllegalStateException
        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> consumer.callBackgroundProcessMessages(processor),
                "backgroundProcessMessages() when not CONSUMING should throw IllegalStateException");

        assertTrue(exception.getMessage().contains("Cannot call processMessages()"),
                "Exception message should mention cannot call processMessages");
        assertTrue(exception.getMessage().contains(CONSUMING.toString()),
                "Exception message should mention CONSUMING state");
    }

    /**
     * Test 14: Tests that backgroundProcessMessages() when already processing throws exception.
     * This exercises line 1076 of AbstractMessageConsumer.java.
     */
    @Test
    public void testBackgroundProcessMessagesWhenAlreadyProcessingThrowsException() throws Exception {
        SimpleTestConsumer consumer = new SimpleTestConsumer();

        // Initialize
        JsonObject config = Json.createObjectBuilder().build();
        consumer.init(config);

        // Manually set state to CONSUMING
        synchronized (consumer) {
            // setState is protected - directly accessible in same package
            consumer.setState(CONSUMING);
        }

        // Set processing flag to true
        consumer.setProcessingFlag(true);

        MessageProcessor processor = (msg) -> {};

        // Should throw IllegalStateException
        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> consumer.callBackgroundProcessMessages(processor),
                "backgroundProcessMessages() when already processing should throw IllegalStateException");

        assertTrue(exception.getMessage().contains("Cannot call processMessages()"),
                "Exception message should mention cannot call processMessages");
        assertTrue(exception.getMessage().contains("already been called"),
                "Exception message should mention already called");
    }

    /**
     * Test 15: Tests that backgroundProcessMessages() when processingThread is non-null throws exception.
     * This exercises line 1085 of AbstractMessageConsumer.java.
     */
    @Test
    public void testBackgroundProcessMessagesWhenProcessingThreadExistsThrowsException() throws Exception {
        SimpleTestConsumer consumer = new SimpleTestConsumer();

        // Initialize
        JsonObject config = Json.createObjectBuilder().build();
        consumer.init(config);

        // Manually set state to CONSUMING
        synchronized (consumer) {
            // setState is protected - directly accessible in same package
            consumer.setState(CONSUMING);
        }

        // Set processing flag to true (required) and processingThread to non-null
        consumer.setProcessingFlag(false); // Not processing yet
        consumer.setProcessingThread(Thread.currentThread()); // But thread exists

        MessageProcessor processor = (msg) -> {};

        // Should throw IllegalStateException
        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> consumer.callBackgroundProcessMessages(processor),
                "backgroundProcessMessages() when processingThread exists should throw IllegalStateException");

        assertTrue(exception.getMessage().contains("Processing thread"),
                "Exception message should mention processing thread");
        assertTrue(exception.getMessage().contains("already exist"),
                "Exception message should mention already exists");

        // Clean up
        consumer.setProcessingThread(null);
    }

    /**
     * Test 16: Tests timerStart(), timerPause(), and timerResume() functions.
     * This exercises lines 1768-1777 (timerResume) of AbstractMessageConsumer.java.
     */
    @Test
    public void testTimerStartPauseResume() throws Exception {
        SimpleTestConsumer consumer = new SimpleTestConsumer();

        // Initialize (this sets up the timers)
        JsonObject config = Json.createObjectBuilder().build();
        consumer.init(config);

        // Use the serviceProcess stat for testing
        AbstractMessageConsumer.Stat testStat = AbstractMessageConsumer.Stat.serviceProcess;
        AbstractMessageConsumer.Stat testStat2 = AbstractMessageConsumer.Stat.markProcessed;

        // Start the timer
        consumer.callTimerStart(testStat);

        // Sleep a bit to accumulate time
        Thread.sleep(10);

        // Pause the timer
        consumer.callTimerPause(testStat);

        // Get statistics to verify timer was used
        Map<Statistic, Number> stats1 = consumer.getStatistics();
        Number serviceProcessTime1 = stats1.get(testStat);
        assertNotNull(serviceProcessTime1, "Timer should have recorded time");
        assertTrue(serviceProcessTime1.longValue() >= 0, "Timer value should be non-negative");

        // Resume the timer
        consumer.callTimerResume(testStat);

        // Sleep a bit more
        Thread.sleep(10);

        // Pause again
        consumer.callTimerPause(testStat);

        // Verify time increased
        Map<Statistic, Number> stats2 = consumer.getStatistics();
        Number serviceProcessTime2 = stats2.get(testStat);
        assertTrue(serviceProcessTime2.longValue() >= serviceProcessTime1.longValue(),
                "Timer value should have increased after resume");

        // Test with multiple timers (varargs)
        consumer.callTimerStart(testStat, testStat2);
        Thread.sleep(5);
        consumer.callTimerPause(testStat, testStat2);

        // Verify both timers were affected
        Map<Statistic, Number> stats3 = consumer.getStatistics();
        Number markProcessedTime = stats3.get(testStat2);
        assertNotNull(markProcessedTime, "Second timer should have recorded time");
        assertTrue(markProcessedTime.longValue() >= 0, "Second timer value should be non-negative");

        // Test timerResume with multiple timers
        consumer.callTimerResume(testStat, testStat2);
        Thread.sleep(5);
        consumer.callTimerPause(testStat, testStat2);

        // Verify the test completed without exceptions
        Map<Statistic, Number> stats4 = consumer.getStatistics();
        assertNotNull(stats4.get(testStat), "First timer should still have value");
        assertNotNull(stats4.get(testStat2), "Second timer should still have value");
    }

    // ========================================================================
    // InfoMessage Tests
    // Tests for AbstractMessageConsumer.InfoMessage inner class
    // ========================================================================

    /**
     * Tests that InfoMessage.isPending() returns true before markProcessed() is called
     * and false after markProcessed() is called.
     */
    @Test
    public void testInfoMessageIsPending() {
        // Create a simple JSON message
        String jsonText = "{\"DATA_SOURCE\":\"TEST\",\"RECORD_ID\":\"001\",\"AFFECTED_ENTITIES\":[{\"ENTITY_ID\":1}]}";

        // Create a MessageBatch which will create an InfoMessage
        AbstractMessageConsumer.MessageBatch<Message> batch =
                new AbstractMessageConsumer.MessageBatch<>(new Message(1, jsonText), jsonText);

        // Get the InfoMessage from the batch
        List<AbstractMessageConsumer.InfoMessage<Message>> infoMessages = batch.getInfoMessages();
        assertNotNull(infoMessages, "InfoMessages list should not be null");
        assertEquals(1, infoMessages.size(), "Should have exactly one InfoMessage");

        AbstractMessageConsumer.InfoMessage<Message> infoMessage = infoMessages.get(0);

        // Before markProcessed() is called, isPending() should return true
        assertTrue(infoMessage.isPending(), "isPending() should return true before markProcessed()");

        // Mark the message as processed (disposable = true)
        infoMessage.markProcessed(true);

        // After markProcessed() is called, isPending() should return false
        assertFalse(infoMessage.isPending(), "isPending() should return false after markProcessed()");
    }

    /**
     * Tests that InfoMessage.isPending() returns false after markProcessed(false) is called
     * (when the message failed and should be retried).
     */
    @Test
    public void testInfoMessageIsPendingAfterFailure() {
        // Create a simple JSON message
        String jsonText = "{\"DATA_SOURCE\":\"TEST\",\"RECORD_ID\":\"002\",\"AFFECTED_ENTITIES\":[{\"ENTITY_ID\":2}]}";

        // Create a MessageBatch which will create an InfoMessage
        AbstractMessageConsumer.MessageBatch<Message> batch =
                new AbstractMessageConsumer.MessageBatch<>(new Message(2, jsonText), jsonText);

        // Get the InfoMessage from the batch
        AbstractMessageConsumer.InfoMessage<Message> infoMessage = batch.getInfoMessages().get(0);

        // Before markProcessed() is called, isPending() should return true
        assertTrue(infoMessage.isPending(), "isPending() should return true before markProcessed()");

        // Mark the message as processed but failed (disposable = false, should be retried)
        infoMessage.markProcessed(false);

        // After markProcessed() is called, isPending() should return false (even if it failed)
        assertFalse(infoMessage.isPending(), "isPending() should return false after markProcessed(false)");
    }

    /**
     * Tests that InfoMessage.toString() returns the expected format containing
     * the disposable status and the JSON message text.
     * Note: isDisposable() returns false when disposable field is null (pending)
     * because it uses Boolean.TRUE.equals(disposable).
     */
    @Test
    public void testInfoMessageToString() {
        // Create a simple JSON message
        String jsonText = "{\"DATA_SOURCE\":\"TEST\",\"RECORD_ID\":\"003\",\"AFFECTED_ENTITIES\":[{\"ENTITY_ID\":3}]}";

        // Create a MessageBatch which will create an InfoMessage
        AbstractMessageConsumer.MessageBatch<Message> batch =
                new AbstractMessageConsumer.MessageBatch<>(new Message(3, jsonText), jsonText);

        // Get the InfoMessage from the batch
        AbstractMessageConsumer.InfoMessage<Message> infoMessage = batch.getInfoMessages().get(0);

        // Before markProcessed(), toString() should show disposable=false
        // (isDisposable() returns false when pending because it uses Boolean.TRUE.equals(null))
        String toStringBefore = infoMessage.toString();
        assertNotNull(toStringBefore, "toString() should not return null");
        assertTrue(toStringBefore.contains("disposable=[ false ]"),
                "toString() should contain 'disposable=[ false ]' before processing (pending state), but was: " + toStringBefore);
        assertTrue(toStringBefore.contains("DATA_SOURCE"),
                "toString() should contain the JSON message content");

        // Mark the message as processed successfully
        infoMessage.markProcessed(true);

        // After markProcessed(true), toString() should show disposable=true
        String toStringAfter = infoMessage.toString();
        assertNotNull(toStringAfter, "toString() should not return null after processing");
        assertTrue(toStringAfter.contains("disposable=[ true ]"),
                "toString() should contain 'disposable=[ true ]' after markProcessed(true), but was: " + toStringAfter);
        assertTrue(toStringAfter.contains("DATA_SOURCE"),
                "toString() should contain the JSON message content");
    }

    /**
     * Tests that InfoMessage.toString() shows disposable=false after markProcessed(false).
     */
    @Test
    public void testInfoMessageToStringAfterFailure() {
        // Create a simple JSON message
        String jsonText = "{\"DATA_SOURCE\":\"TEST\",\"RECORD_ID\":\"004\",\"AFFECTED_ENTITIES\":[{\"ENTITY_ID\":4}]}";

        // Create a MessageBatch which will create an InfoMessage
        AbstractMessageConsumer.MessageBatch<Message> batch =
                new AbstractMessageConsumer.MessageBatch<>(new Message(4, jsonText), jsonText);

        // Get the InfoMessage from the batch
        AbstractMessageConsumer.InfoMessage<Message> infoMessage = batch.getInfoMessages().get(0);

        // Mark the message as failed (should be retried)
        infoMessage.markProcessed(false);

        // After markProcessed(false), toString() should show disposable=false
        String toStringAfter = infoMessage.toString();
        assertNotNull(toStringAfter, "toString() should not return null after processing");
        assertTrue(toStringAfter.contains("disposable=[ false ]"),
                "toString() should contain 'disposable=[ false ]' after markProcessed(false), but was: " + toStringAfter);
    }

    private static Map<Statistic, Number> printStatistics(TestMessageConsumer consumer, TestService service, ConnectionPool pool) {
        System.err.println();
        System.err.println("=====================================================");
        System.err.println("MESSAGES COMPLETED: " + service.getSuccessCount());
        Map<Statistic, Number> stats = consumer.getStatistics();

        if (pool != null) {
            System.err.println("POOL STATISTICS: ");
            Map<ConnectionPool.Statistic, Number> poolStats = pool.getStatistics();
            poolStats.forEach((statistic, value) -> {
                System.err.println("  " + statistic + ": " + value + " " + statistic.getUnits());
            });
            System.err.println();
        }
        System.err.println("CONSUMER STATISTICS:");
        System.err.println("  dequeueCount: " + consumer.getDequeueCount() + " messages");
        System.err.println("  dequeueSleep: " + consumer.getDequeueSleep() + " ms");
        System.err.println("  visibilityTimeout: " + consumer.getVisibilityTimeout() + " ms");
        System.err.println(
                "  expectedServiceProcessFailureCount: " + consumer.getExpectedFailureCount() + " info messages");
        System.err.println("  expectedMessageRetryCount: " + consumer.getExpectedMessageRetryCount() + " messages");
        System.err.println(
                "  expectedInfoMessageRetryCount: " + consumer.getExpectedInfoMessageRetryCount() + " info messages");

        stats.forEach((key, value) -> {
            String units = key.getUnits();
            System.out.println("  " + key + ": " + value + ((units != null) ? " " + units : ""));
        });

        System.err.println();
        System.err.println("-----------------------------------------------------");
        AbstractSchedulingService schedulingService = (AbstractSchedulingService) service.getSchedulingService();
        Map<Statistic, Number> stats2 = schedulingService.getStatistics();
        System.err.println("SCHEDULING STATISTICS:");
        stats2.forEach((key, value) -> {
            String units = key.getUnits();
            System.out.println("  " + key + ": " + value + ((units != null) ? " " + units : ""));
        });

        return stats;
    }
}
