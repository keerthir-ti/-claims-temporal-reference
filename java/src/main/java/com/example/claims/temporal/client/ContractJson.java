package com.example.claims.temporal.client;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.deser.DeserializationProblemHandler;
import io.temporal.common.converter.DataConverter;
import io.temporal.common.converter.DefaultDataConverter;
import io.temporal.common.converter.JacksonJsonPayloadConverter;
import java.io.IOException;

/**
 * One ObjectMapper for every Temporal payload, shared by Java workers, the bridge and the tests.
 *
 * <p>Tolerant-reader rules (mirrored in python/claims/contracts/base.py):
 * <ul>
 *   <li>unknown properties are ignored, so contracts can evolve additively;</li>
 *   <li>an unknown enum value maps to the enum's UNKNOWN constant instead of failing the
 *       workflow task (a failed decode inside a workflow task blocks the workflow until a fix is
 *       deployed).</li>
 * </ul>
 */
public final class ContractJson {

  public static final ObjectMapper MAPPER = newMapper();

  private ContractJson() {}

  public static ObjectMapper newMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.findAndRegisterModules();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    mapper.addHandler(new UnknownEnumFallback());
    return mapper;
  }

  public static DataConverter dataConverter() {
    return DefaultDataConverter.newDefaultInstance()
        .withPayloadConverterOverrides(new JacksonJsonPayloadConverter(MAPPER));
  }

  static final class UnknownEnumFallback extends DeserializationProblemHandler {
    @Override
    public Object handleWeirdStringValue(
        DeserializationContext ctxt, Class<?> targetType, String valueToConvert, String failureMsg)
        throws IOException {
      Object fallback = unknownConstant(targetType);
      return fallback != null ? fallback : NOT_HANDLED;
    }

    @Override
    public Object handleInstantiationProblem(
        DeserializationContext ctxt, Class<?> instClass, Object argument, Throwable t)
        throws IOException {
      // Generated enums use a @JsonCreator fromValue(...) that throws IllegalArgumentException.
      Object fallback = unknownConstant(instClass);
      return fallback != null ? fallback : NOT_HANDLED;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object unknownConstant(Class<?> type) {
      if (type == null || !type.isEnum()) {
        return null;
      }
      try {
        return Enum.valueOf((Class<? extends Enum>) type, "UNKNOWN");
      } catch (IllegalArgumentException e) {
        return null;
      }
    }
  }
}
