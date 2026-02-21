package com.senzing.listener.service.scheduling;

import com.senzing.listener.service.exception.ServiceExecutionException;
import org.junit.jupiter.api.*;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link TaskBuilder} and {@link DefaultScheduler}.
 * Focuses on builder API and scheduler lifecycle.
 */
class TaskBuilderAndSchedulerTest {

    private static class NoOpTaskHandler implements TaskHandler {
        @Override
        public Boolean waitUntilReady(long timeoutMillis) {
            return Boolean.TRUE;
        }

        @Override
        public void handleTask(String action, Map<String, Object> parameters,
                             int multiplicity, Scheduler followUpScheduler) {
            // No-op
        }
    }

    @Test
    void testCreateScheduler() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);
        assertNotNull(scheduler);
        assertNull(scheduler.getTaskGroup());

        service.destroy();
    }

    @Test
    void testCreateSchedulerWithTaskGroup() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        TaskGroup group = new TaskGroup();
        DefaultScheduler scheduler = new DefaultScheduler(service, group);

        assertEquals(group, scheduler.getTaskGroup());

        service.destroy();
    }

    @Test
    void testSchedulerNullServiceThrows() {
        assertThrows(NullPointerException.class, () -> new DefaultScheduler(null));
    }

    @Test
    void testCreateTaskBuilder() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);
        TaskBuilder builder = scheduler.createTaskBuilder("TEST_ACTION");

        assertNotNull(builder);

        service.destroy();
    }

    @Test
    void testTaskBuilderSimpleParameters() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        TaskGroup group = new TaskGroup();
        DefaultScheduler scheduler = new DefaultScheduler(service, group);

        scheduler.createTaskBuilder("ACTION")
            .parameter("str", "value")
            .parameter("int", 42)
            .parameter("long", 100L)
            .parameter("float", 3.14f)
            .parameter("double", 2.718)
            .parameter("bool", true)
            .schedule(true);

        assertEquals(1, scheduler.getPendingCount());
        scheduler.commit();
        assertEquals(1, group.getTaskCount());

        service.destroy();
    }

    @Test
    void testTaskBuilderResources() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        TaskGroup group = new TaskGroup();
        DefaultScheduler scheduler = new DefaultScheduler(service, group);

        scheduler.createTaskBuilder("ACTION")
            .resource("ENTITY", "123")
            .resource("RECORD", "DS1", "R001")
            .resource("REPORT", "summary")
            .schedule(true);

        scheduler.commit();
        assertEquals(1, group.getTaskCount());

        service.destroy();
    }

    @Test
    void testTaskBuilderListParameter() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);

        scheduler.createTaskBuilder("ACTION")
            .listParameter("list")
                .add("str1")
                .add("str2", "str3") // Adds sub-list
                .add(100)
                .add(200, 300) // Adds sub-list
                .add(1.5f)
                .add(2.5, 3.5) // Adds sub-list
                .add(true, false) // Adds sub-list
                .endList()
            .schedule(true);

        assertEquals(1, scheduler.getPendingCount());

        service.destroy();
    }

    @Test
    void testTaskBuilderMapParameter() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);

        scheduler.createTaskBuilder("ACTION")
            .mapParameter("map")
                .put("str", "value")
                .put("int", 42)
                .put("long", 100L)
                .put("float", 1.5f)
                .put("double", 2.5)
                .put("bool", true)
                .put("list", "a", "b", "c") // Adds list value
                .endMap()
            .schedule(true);

        assertEquals(1, scheduler.getPendingCount());

        service.destroy();
    }

    @Test
    void testListParameterWithJsonTypes() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);

        JsonArray jsonArray = Json.createArrayBuilder().add(1).add(2).build();
        JsonObject jsonObj = Json.createObjectBuilder().add("key", "val").build();

        scheduler.createTaskBuilder("ACTION")
            .listParameter("list")
                .add(jsonArray)
                .add(jsonObj)
                .endList()
            .schedule(true);

        assertEquals(1, scheduler.getPendingCount());

        service.destroy();
    }

    @Test
    void testMapParameterWithJsonTypes() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);

        JsonArray jsonArray = Json.createArrayBuilder().add(1).add(2).build();
        JsonObject jsonObj = Json.createObjectBuilder().add("nested", "val").build();

        scheduler.createTaskBuilder("ACTION")
            .mapParameter("map")
                .put("arr", jsonArray)
                .put("obj", jsonObj)
                .endMap()
            .schedule(true);

        assertEquals(1, scheduler.getPendingCount());

        service.destroy();
    }

    @Test
    void testListBuilderEndListTwiceThrows() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);
        TaskBuilder.ListParamBuilder listBuilder = scheduler.createTaskBuilder("ACTION")
            .listParameter("list");

        listBuilder.add("value");
        TaskBuilder builder = listBuilder.endList();

        assertNotNull(builder);
        assertThrows(IllegalStateException.class, () -> listBuilder.add("another"));

        service.destroy();
    }

    @Test
    void testMapBuilderNullKeyThrows() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);
        TaskBuilder.MapParamBuilder mapBuilder = scheduler.createTaskBuilder("ACTION")
            .mapParameter("map");

        assertThrows(NullPointerException.class, () -> mapBuilder.put(null, "value"));

        service.destroy();
    }

    @Test
    void testMapBuilderDuplicateKeyThrows() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);
        TaskBuilder.MapParamBuilder mapBuilder = scheduler.createTaskBuilder("ACTION")
            .mapParameter("map");

        mapBuilder.put("key", "value1");
        assertThrows(IllegalArgumentException.class, () -> mapBuilder.put("key", "value2"));

        service.destroy();
    }

    @Test
    void testSchedulerCommit() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);

        scheduler.createTaskBuilder("ACTION1").schedule(true);
        scheduler.createTaskBuilder("ACTION2").schedule(true);
        scheduler.createTaskBuilder("ACTION3").schedule(true);

        assertEquals(3, scheduler.getPendingCount());

        int count = scheduler.commit();
        assertEquals(3, count);
        assertEquals(0, scheduler.getPendingCount());

        service.destroy();
    }

    @Test
    void testSchedulerCommitIsIdempotent() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);
        scheduler.createTaskBuilder("ACTION").schedule(true);

        int count1 = scheduler.commit();
        assertEquals(1, count1);

        int count2 = scheduler.commit();
        assertEquals(0, count2);

        service.destroy();
    }

    @Test
    void testSchedulerAfterCommitThrows() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);
        scheduler.commit();

        assertThrows(IllegalStateException.class, () -> scheduler.createTaskBuilder("ACTION"));

        service.destroy();
    }

    @Test
    void testSchedulerWithTaskGroupClosesOnCommit() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        TaskGroup group = new TaskGroup();
        DefaultScheduler scheduler = new DefaultScheduler(service, group);

        assertEquals(TaskGroup.State.OPEN, group.getState());

        scheduler.createTaskBuilder("ACTION").schedule(true);
        scheduler.commit();

        assertNotEquals(TaskGroup.State.OPEN, group.getState());

        service.destroy();
    }

    @Test
    void testScheduleWithWrongTaskGroupThrows() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        TaskGroup group1 = new TaskGroup();
        TaskGroup group2 = new TaskGroup();

        DefaultScheduler scheduler = new DefaultScheduler(service, group1);

        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), group2, true);

        assertThrows(IllegalArgumentException.class, () -> scheduler.schedule(task));

        service.destroy();
    }

    @Test
    void testTaskBuilderNullActionThrows() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);

        assertThrows(NullPointerException.class, () -> scheduler.createTaskBuilder(null));

        service.destroy();
    }

    // ========================================================================
    // ListParamBuilder Additional Type Coverage
    // ========================================================================

    @Test
    void testListBuilderBigIntegerAndBigDecimal() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);

        scheduler.createTaskBuilder("ACTION")
            .listParameter("list")
                .add(new java.math.BigInteger("123456789"))
                .add(new java.math.BigInteger("111"), new java.math.BigInteger("222"))
                .add(new java.math.BigDecimal("3.14159"))
                .add(new java.math.BigDecimal("1.1"), new java.math.BigDecimal("2.2"))
                .endList()
            .schedule(true);

        assertEquals(1, scheduler.getPendingCount());

        service.destroy();
    }

    @Test
    void testListBuilderBooleanValues() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);

        scheduler.createTaskBuilder("ACTION")
            .listParameter("list")
                .add(Boolean.TRUE)
                .add(Boolean.FALSE, Boolean.TRUE)
                .endList()
            .schedule(true);

        assertEquals(1, scheduler.getPendingCount());

        service.destroy();
    }

    @Test
    void testListBuilderMultipleValuesVariants() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);

        scheduler.createTaskBuilder("ACTION")
            .listParameter("list")
                .add(1, 2, 3) // Integer multi-value
                .add(100L, 200L, 300L) // Long multi-value
                .add(1.1f, 2.2f) // Float multi-value
                .add(3.14, 2.718, 1.414) // Double multi-value
                .endList()
            .schedule(true);

        assertEquals(1, scheduler.getPendingCount());

        service.destroy();
    }

    @Test
    void testListBuilderZeroLengthArrays() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);

        scheduler.createTaskBuilder("ACTION")
            .listParameter("list")
                .add(new String[0])
                .add(new Integer[0])
                .add(new Long[0])
                .add(new Float[0])
                .add(new Double[0])
                .add(new Boolean[0])
                .add(new java.math.BigInteger[0])
                .add(new java.math.BigDecimal[0])
                .endList()
            .schedule(true);

        assertEquals(1, scheduler.getPendingCount());

        service.destroy();
    }

    // ========================================================================
    // MapParamBuilder Additional Type Coverage
    // ========================================================================

    @Test
    void testMapBuilderBigIntegerAndBigDecimal() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);

        scheduler.createTaskBuilder("ACTION")
            .mapParameter("map")
                .put("bigInt1", new java.math.BigInteger("999"))
                .put("bigInt2", new java.math.BigInteger("111"), new java.math.BigInteger("222"))
                .put("bigDec1", new java.math.BigDecimal("9.99"))
                .put("bigDec2", new java.math.BigDecimal("1.1"), new java.math.BigDecimal("2.2"))
                .endMap()
            .schedule(true);

        assertEquals(1, scheduler.getPendingCount());

        service.destroy();
    }

    @Test
    void testMapBuilderBooleanValues() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);

        scheduler.createTaskBuilder("ACTION")
            .mapParameter("map")
                .put("flag1", Boolean.TRUE)
                .put("flags", Boolean.TRUE, Boolean.FALSE)
                .endMap()
            .schedule(true);

        assertEquals(1, scheduler.getPendingCount());

        service.destroy();
    }

    @Test
    void testMapBuilderMultipleValuesVariants() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);

        scheduler.createTaskBuilder("ACTION")
            .mapParameter("map")
                .put("ints", 1, 2, 3)
                .put("longs", 100L, 200L)
                .put("floats", 1.1f, 2.2f, 3.3f)
                .put("doubles", 1.1, 2.2)
                .endMap()
            .schedule(true);

        assertEquals(1, scheduler.getPendingCount());

        service.destroy();
    }

    @Test
    void testMapBuilderZeroLengthArrays() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);

        scheduler.createTaskBuilder("ACTION")
            .mapParameter("map")
                .put("empty1", new String[0])
                .put("empty2", new Integer[0])
                .put("empty3", new Long[0])
                .put("empty4", new Float[0])
                .put("empty5", new Double[0])
                .put("empty6", new Boolean[0])
                .put("empty7", new java.math.BigInteger[0])
                .put("empty8", new java.math.BigDecimal[0])
                .endMap()
            .schedule(true);

        assertEquals(1, scheduler.getPendingCount());

        service.destroy();
    }

    // ========================================================================
    // TaskBuilder Additional Coverage
    // ========================================================================

    @Test
    void testTaskBuilderParameterWithJsonArray() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);
        JsonArray jsonArray = Json.createArrayBuilder().add(1).add(2).build();

        scheduler.createTaskBuilder("ACTION")
            .parameter("array", jsonArray)
            .schedule(true);

        assertEquals(1, scheduler.getPendingCount());

        service.destroy();
    }

    @Test
    void testTaskBuilderParameterWithJsonObject() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);
        JsonObject jsonObj = Json.createObjectBuilder().add("key", "val").build();

        scheduler.createTaskBuilder("ACTION")
            .parameter("obj", jsonObj)
            .schedule(true);

        assertEquals(1, scheduler.getPendingCount());

        service.destroy();
    }

    @Test
    void testTaskBuilderDuplicateParameterNameThrows() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);

        assertThrows(IllegalArgumentException.class, () -> {
            scheduler.createTaskBuilder("ACTION")
                .parameter("dup", "value1")
                .parameter("dup", "value2"); // Duplicate key
        });

        service.destroy();
    }

    @Test
    void testTaskBuilderAfterCommitCreateTaskBuilderThrows() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);
        scheduler.commit(); // Commit scheduler

        // Cannot create new task builders after commit
        assertThrows(IllegalStateException.class, () -> scheduler.createTaskBuilder("ACTION"));

        service.destroy();
    }

    @Test
    void testMapBuilderEndMapTwiceThrows() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);
        TaskBuilder.MapParamBuilder mapBuilder = scheduler.createTaskBuilder("ACTION")
            .mapParameter("map");

        mapBuilder.put("key", "value");
        TaskBuilder builder = mapBuilder.endMap();

        assertNotNull(builder);
        assertThrows(IllegalStateException.class, () -> mapBuilder.put("key2", "val2"));
        assertThrows(IllegalStateException.class, () -> mapBuilder.endMap());

        service.destroy();
    }

    @Test
    void testListBuilderAddAfterEndListThrows() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);
        TaskBuilder.ListParamBuilder listBuilder = scheduler.createTaskBuilder("ACTION")
            .listParameter("list");

        listBuilder.add("val");
        listBuilder.endList();

        assertThrows(IllegalStateException.class, () -> listBuilder.add(100));
        assertThrows(IllegalStateException.class, () -> listBuilder.add(100L));
        assertThrows(IllegalStateException.class, () -> listBuilder.add(1.5f));
        assertThrows(IllegalStateException.class, () -> listBuilder.add(1.5));
        assertThrows(IllegalStateException.class, () -> listBuilder.add(true));

        service.destroy();
    }

    @Test
    void testMapBuilderAddAfterEndMapThrows() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);
        TaskBuilder.MapParamBuilder mapBuilder = scheduler.createTaskBuilder("ACTION")
            .mapParameter("map");

        mapBuilder.put("key1", "val");
        mapBuilder.endMap();

        assertThrows(IllegalStateException.class, () -> mapBuilder.put("k", 100));
        assertThrows(IllegalStateException.class, () -> mapBuilder.put("k", 100L));
        assertThrows(IllegalStateException.class, () -> mapBuilder.put("k", 1.5f));
        assertThrows(IllegalStateException.class, () -> mapBuilder.put("k", 1.5));
        assertThrows(IllegalStateException.class, () -> mapBuilder.put("k", true));

        service.destroy();
    }

    // ========================================================================
    // TaskBuilder Missing Method Coverage
    // ========================================================================

    @Test
    void testTaskBuilderParameterBigIntegerVariants() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);

        scheduler.createTaskBuilder("ACTION")
            .parameter("single", new java.math.BigInteger("123"))
            .parameter("multi", new java.math.BigInteger("111"), new java.math.BigInteger("222"))
            .schedule(true);

        assertEquals(1, scheduler.getPendingCount());

        service.destroy();
    }

    @Test
    void testTaskBuilderParameterBigDecimalVariants() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);

        scheduler.createTaskBuilder("ACTION")
            .parameter("single", new java.math.BigDecimal("3.14"))
            .parameter("multi", new java.math.BigDecimal("1.1"), new java.math.BigDecimal("2.2"))
            .schedule(true);

        assertEquals(1, scheduler.getPendingCount());

        service.destroy();
    }

    @Test
    void testTaskBuilderParameterMultiValueVariants() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);

        scheduler.createTaskBuilder("ACTION")
            .parameter("strs", "a", "b", "c")
            .parameter("ints", 1, 2, 3)
            .parameter("longs", 100L, 200L)
            .parameter("floats", 1.1f, 2.2f, 3.3f)
            .parameter("doubles", 1.5, 2.5)
            .parameter("bools", true, false, true)
            .schedule(true);

        assertEquals(1, scheduler.getPendingCount());

        service.destroy();
    }

    @Test
    void testTaskBuilderResourceWithObjects() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);

        scheduler.createTaskBuilder("ACTION")
            .resource("ENTITY", 12345L, "suffix")
            .resource("RECORD", Integer.valueOf(100), "DS1")
            .schedule(true);

        assertEquals(1, scheduler.getPendingCount());

        service.destroy();
    }

    @Test
    void testTaskBuilderScheduleNoArg() throws Exception {
        MockSchedulingService service = new MockSchedulingService();
        service.init(null, new NoOpTaskHandler());

        DefaultScheduler scheduler = new DefaultScheduler(service);

        scheduler.createTaskBuilder("ACTION")
            .parameter("test", "value")
            .schedule(); // Uses default allowCollapse=true

        assertEquals(1, scheduler.getPendingCount());

        service.destroy();
    }
}


