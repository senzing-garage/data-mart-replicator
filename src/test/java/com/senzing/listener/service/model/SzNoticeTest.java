package com.senzing.listener.service.model;

import com.senzing.util.JsonUtilities;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link SzNotice}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SzNoticeTest {
  /**
   * The JSON property key for serializing and deserializing the code property
   * for instances of this class.
   */
  public static final String CODE_KEY = "code";

  /**
   * The JSON property key for deserializing the code property when parsing raw
   * Senzing INFO message JSON to construct instances of this class.
   */
  public static final String RAW_CODE_KEY = "CODE";

  /**
   * The JSON property key for serializing and deserializing the description
   * property for instances of this class.
   */
  public static final String DESCRIPTION_KEY = "description";

  /**
   * The JSON property key for deserializing the description property when
   * parsing raw Senzing INFO message JSON to construct instances of this class.
   */
  public static final String RAW_DESCRIPTION_KEY = "DESCRIPTION";

  public List<Arguments> getPropertyParameters() {
    List<Arguments> result = new LinkedList<>();
    result.add(arguments("FOO", "Foo description"));
    result.add(arguments("BAR", "Bar description"));
    result.add(arguments(null, null));
    result.add(arguments(null, "Some description"));
    result.add(arguments("PHOO", null));
    return result;
  }

  @Test
  public void defaultConstructTest() {
    SzNotice notice = new SzNotice();
    assertEquals(null, notice.getCode(), "Code is not null");
    assertEquals(null, notice.getDescription(),
                 "Description is not null");
  }

  @ParameterizedTest
  @MethodSource("getPropertyParameters")
  public void constructTest(String code, String description) {
    SzNotice notice = new SzNotice(code, description);
    assertEquals(code, notice.getCode(), "Code is not as expected");
    assertEquals(description, notice.getDescription(),
                 "Description is not as expected");
  }

  @ParameterizedTest
  @MethodSource("getPropertyParameters")
  public void propertyTest(String code, String description) {
    SzNotice notice = new SzNotice();
    notice.setCode(code);
    notice.setDescription(description);
    assertEquals(code, notice.getCode(), "Code is not as expected");
    assertEquals(description, notice.getDescription(),
                 "Description is not as expected");
  }

  @ParameterizedTest
  @MethodSource("getPropertyParameters")
  public void toJsonTextTest(String code, String description) {
    SzNotice notice = new SzNotice();
    notice.setCode(code);
    notice.setDescription(description);

    String jsonText = notice.toJsonText();
    JsonObject actual = JsonUtilities.parseJsonObject(jsonText);

    JsonObjectBuilder job = Json.createObjectBuilder();
    JsonUtilities.add(job, CODE_KEY, code);
    JsonUtilities.add(job, DESCRIPTION_KEY, description);
    JsonObject expected = job.build();

    assertEquals(expected, actual, "JSON not as expected");
  }

  @ParameterizedTest
  @MethodSource("getPropertyParameters")
  public void toJsonTextPrettyPrintTest(String code, String description) {
    SzNotice notice = new SzNotice(code, description);

    String jsonText = notice.toJsonText(true);
    assertNotNull(jsonText, "Pretty-printed JSON text should not be null");

    // Verify it can be parsed back
    JsonObject parsed = JsonUtilities.parseJsonObject(jsonText);
    assertNotNull(parsed, "Parsed JSON should not be null");
  }

  @ParameterizedTest
  @MethodSource("getPropertyParameters")
  public void toJsonObjectTest(String code, String description) {
    SzNotice notice = new SzNotice();
    notice.setCode(code);
    notice.setDescription(description);

    JsonObject actual = notice.toJsonObject();

    JsonObjectBuilder job = Json.createObjectBuilder();
    JsonUtilities.add(job, CODE_KEY, code);
    JsonUtilities.add(job, DESCRIPTION_KEY, description);
    JsonObject expected = job.build();

    assertEquals(expected, actual, "JSON not as expected");
  }

  @ParameterizedTest
  @MethodSource("getPropertyParameters")
  public void toJsonObjectBuilderWithNullTest(String code, String description) {
    SzNotice notice = new SzNotice(code, description);

    JsonObjectBuilder builder = notice.toJsonObjectBuilder(null);
    assertNotNull(builder, "Builder should not be null");
    assertNotNull(builder.build(), "Built JSON should not be null");
  }

  @ParameterizedTest
  @MethodSource("getPropertyParameters")
  public void toJsonObjectBuilderWithExistingTest(String code, String description) {
    SzNotice notice = new SzNotice(code, description);

    JsonObjectBuilder existingBuilder = Json.createObjectBuilder();
    existingBuilder.add("existingKey", "existingValue");

    JsonObjectBuilder builder = notice.toJsonObjectBuilder(existingBuilder);
    JsonObject result = builder.build();

    assertTrue(result.containsKey("existingKey"), "Should retain existing key");
    assertEquals("existingValue", result.getString("existingKey"));
  }

  @ParameterizedTest
  @MethodSource("getPropertyParameters")
  public void fromJsonTextTest(String code, String description) {

    JsonObjectBuilder job = Json.createObjectBuilder();
    JsonUtilities.add(job, CODE_KEY, code);
    JsonUtilities.add(job, DESCRIPTION_KEY, description);
    JsonObject jsonObject = job.build();

    String jsonText = JsonUtilities.toJsonText(jsonObject);

    SzNotice notice = SzNotice.fromJson(jsonText);

    assertEquals(code, notice.getCode(), "Code is not as expected");
    assertEquals(description, notice.getDescription(),
                 "Description is not as expected");
  }

  @ParameterizedTest
  @MethodSource("getPropertyParameters")
  public void fromJsonObjectTest(String code, String description) {

    JsonObjectBuilder job = Json.createObjectBuilder();
    JsonUtilities.add(job, CODE_KEY, code);
    JsonUtilities.add(job, DESCRIPTION_KEY, description);
    JsonObject jsonObject = job.build();

    SzNotice notice = SzNotice.fromJson(jsonObject);

    assertEquals(code, notice.getCode(), "Code is not as expected");
    assertEquals(description, notice.getDescription(),
                 "Description is not as expected");
  }

  @Test
  public void fromJsonNullTextTest() {
    SzNotice notice = SzNotice.fromJson((String) null);
    assertNull(notice, "fromJson with null text should return null");
  }

  @Test
  public void fromJsonNullObjectTest() {
    SzNotice notice = SzNotice.fromJson((JsonObject) null);
    assertNull(notice, "fromJson with null object should return null");
  }

  @Test
  public void fromJsonMissingCodePropertyTest() {
    JsonObject jsonObject = Json.createObjectBuilder()
            .add(DESCRIPTION_KEY, "Some description")
            .build();

    assertThrows(IllegalArgumentException.class,
            () -> SzNotice.fromJson(jsonObject),
            "Should throw when code property is missing");
  }

  @Test
  public void fromJsonRoundTripTest() {
    SzNotice original = new SzNotice("TEST_CODE", "Test description");

    String jsonText = original.toJsonText();
    SzNotice parsed = SzNotice.fromJson(jsonText);

    assertEquals(original, parsed, "Round-trip should produce equal object");
  }

  @Test
  public void fromJsonObjectRoundTripTest() {
    SzNotice original = new SzNotice("CODE", "Description");

    JsonObject jsonObject = original.toJsonObject();
    SzNotice parsed = SzNotice.fromJson(jsonObject);

    assertEquals(original, parsed, "Round-trip should produce equal object");
  }

  @ParameterizedTest
  @MethodSource("getPropertyParameters")
  public void fromRawJsonTextTest(String code, String description) {

    JsonObjectBuilder job = Json.createObjectBuilder();
    JsonUtilities.add(job, RAW_CODE_KEY, code);
    JsonUtilities.add(job, RAW_DESCRIPTION_KEY, description);
    JsonObject jsonObject = job.build();

    String jsonText = JsonUtilities.toJsonText(jsonObject);

    SzNotice notice = SzNotice.fromRawJson(jsonText);

    assertEquals(code, notice.getCode(), "Code is not as expected");
    assertEquals(description, notice.getDescription(),
                 "Description is not as expected");
  }

  @ParameterizedTest
  @MethodSource("getPropertyParameters")
  public void fromRawJsonObjectTest(String code, String description) {

    JsonObjectBuilder job = Json.createObjectBuilder();
    JsonUtilities.add(job, RAW_CODE_KEY, code);
    JsonUtilities.add(job, RAW_DESCRIPTION_KEY, description);
    JsonObject jsonObject = job.build();

    SzNotice notice = SzNotice.fromRawJson(jsonObject);

    assertEquals(code, notice.getCode(), "Code is not as expected");
    assertEquals(description, notice.getDescription(),
                 "Description is not as expected");
  }

  @Test
  public void fromRawJsonNullTextTest() {
    SzNotice notice = SzNotice.fromRawJson((String) null);
    assertNull(notice, "fromRawJson with null text should return null");
  }

  @Test
  public void fromRawJsonNullObjectTest() {
    SzNotice notice = SzNotice.fromRawJson((JsonObject) null);
    assertNull(notice, "fromRawJson with null object should return null");
  }

  @ParameterizedTest
  @MethodSource("getPropertyParameters")
  public void equalsTest(String code, String description) {
    SzNotice notice1 = new SzNotice(code, description);
    SzNotice notice2 = new SzNotice(code, description);
    assertEquals(notice1, notice2, "Equals method not equal");
  }

  @Test
  public void equalsSameInstanceTest() {
    SzNotice notice = new SzNotice("CODE", "description");
    assertEquals(notice, notice, "Notice should equal itself");
  }

  @Test
  public void notEqualsNullTest() {
    SzNotice notice = new SzNotice("CODE", "description");
    assertNotEquals(notice, null, "Notice should not equal null");
  }

  @Test
  public void notEqualsDifferentClassTest() {
    SzNotice notice = new SzNotice("CODE", "description");
    assertNotEquals(notice, "not a notice", "Notice should not equal different class");
  }

  @ParameterizedTest
  @MethodSource("getPropertyParameters")
  public void notEqualsTest(String code, String description) {
    SzNotice notice1 = new SzNotice(code, description);

    if (code != null) {
      code = code + "A";
    }
    if (description != null) {
      description = description + "A";
    }
    if ((code == null) && (description == null)) {
      description = "A";
    }

    SzNotice notice2 = new SzNotice(code, description);

    assertNotEquals(notice1, notice2, "Equals method equal");
  }

  @ParameterizedTest
  @MethodSource("getPropertyParameters")
  public void hashCodeConsistencyTest(String code, String description) {
    SzNotice notice1 = new SzNotice(code, description);
    SzNotice notice2 = new SzNotice(code, description);

    if (notice1.equals(notice2)) {
      assertEquals(notice1.hashCode(), notice2.hashCode(),
              "Equal objects must have equal hash codes");
    }
  }

  @ParameterizedTest
  @MethodSource("getPropertyParameters")
  public void toStringTest(String code, String description) {
    SzNotice notice = new SzNotice(code, description);
    assertDoesNotThrow(() -> notice.toString(),
            "toString should not throw exception");
    assertNotNull(notice.toString(), "toString should not return null");
  }

}
