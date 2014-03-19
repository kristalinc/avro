package org.apache.avro.compiler.specific;

import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: ddmoore
 * Date: 8/25/13
 * Time: 9:40 PM
 */
public class JSONObjectCompiler extends SpecificCompiler {
  private static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);
  private String basePackage;
  private String copyrightNotice;

  public JSONObjectCompiler(Schema schema) {
    super(schema);
  }

  public List<Schema.Field> getRequiredFields(Schema schema) {
    List<Schema.Field> requiredFields = new ArrayList<Schema.Field>();
    for (Schema.Field field : schema.getFields()) {
      if (isRequiredOnCreate(field)) requiredFields.add(field);
    }
    return requiredFields;
  }

  public List<Schema.Field> getSettableFields(Schema schema) {
    List<Schema.Field> settableFields = new ArrayList<Schema.Field>();
    for (Schema.Field field : schema.getFields()) {
      if (!isReadOnly(field)) settableFields.add(field);
    }
    return settableFields;
  }

  public String generateBuilderMethod(Schema schema, Schema.Field field) {
    String setMethodName = generateSetMethod(schema, field);
    String baseMethodName = setMethodName.substring(3);
    return Character.toLowerCase(baseMethodName.charAt(0)) + baseMethodName.substring(1);
  }

  public String jsonAccessor(Schema schema) {
    switch (schema.getType()) {
      case RECORD:
      case ENUM:
      case FIXED:
        return "optJSONObject";
      case ARRAY:
        return "optJSONArray";
      case MAP:
        return "optJSONObject";
      case UNION:
        List<Schema> types = schema.getTypes(); // elide unions with null
        if ((types.size() == 2) && types.contains(NULL_SCHEMA))
          return jsonAccessor(types.get(types.get(0).equals(NULL_SCHEMA) ? 1 : 0));
        return "optJSONObject";
      case STRING:
        return "optString";
//      case BYTES:   return "java.nio.ByteBuffer";
      case INT:     return "optInt";
      case LONG:    return "optLong";
      case FLOAT:   return "optDouble";
      case DOUBLE:  return "optDouble";
      case BOOLEAN: return "optBoolean";
//      case NULL:    return "java.lang.Void";
      default: throw new RuntimeException("Unknown type: "+schema);
    }
  }

  public String generateValidityCheck(Schema.Field field, String fieldName) {
    String check = "";
    Schema schema = field.schema();
    switch (schema.getType()) {
      case RECORD:
      case ENUM:
      case FIXED:
      case MAP:
        break;
      case ARRAY:
        break;
      case UNION:
        return "";
      case STRING:
        JsonNode length = field.getJsonProp("length");
        if (length != null) {
          check = "if (" + fieldName + " != null && " + fieldName + ".length() > " + length.asLong() + ") throw new IllegalArgumentException(\"Maximum string length exceeded for '" + fieldName + "'\");";
        }
        break;
      case BYTES:   break;
      case INT:
      case LONG:
        JsonNode max = field.getJsonProp("max");
        JsonNode min = field.getJsonProp("min");
        if (min != null || max != null) {
          check = "if (" + fieldName + " != null && ";
          if (min != null) {
            check += fieldName + " < " + min.asLong();
            if (max != null) check += " && ";
          }
          if (max != null) {
            check += fieldName + " > " + max.asLong();
          }
          check += ") throw new IllegalArgumentException(\"Invalid value for '" + fieldName + "'\");";
        }
        break;
      case FLOAT:
      case DOUBLE:
        break;
      case BOOLEAN: break;
      case NULL:    break;
      default: throw new RuntimeException("Unknown type: "+schema);
    }
    return check;
  }

  public String jsonFallback(Schema.Field field){
    String fallback = "";
    JsonNode defaultValue = field.defaultValue();
    if (defaultValue != null) {
      fallback = ", "+ defaultValue.toString();
      if (field.schema().getType() != Schema.Type.STRING) {
        fallback = fallback.replace("\"", "");
      }
    } else if (field.schema().getType() == Schema.Type.STRING) {
      fallback = ", null";
    }
    return fallback;
  }

//  JsonNode length = field.getJsonProp("length");
//  if (length != null) {
//    check = "if (" + fieldName + " != null && " + fieldName + ".length() > " + length.asLong() + ") throw new IllegalArgumentException(\"Maximum string length exceeded for '" + fieldName + "'\");";
//  }

//  public String convertToJson(Schema schema, String objName) {
//    switch (schema.getType()) {
//      case RECORD:
//      case ENUM:
//      case FIXED:
//        return "java.lang.String item = " + objName + ".toString()";
//      case ARRAY:
//        return "???";
//      case UNION:
//        List<Schema> types = schema.getTypes(); // elide unions with null
//        if ((types.size() == 2) && types.contains(NULL_SCHEMA))
//          return jsonAccessor(types.get(types.get(0).equals(NULL_SCHEMA) ? 1 : 0));
//        return "optJSONObject";
//      case STRING:
//        return "getString";
////      case BYTES:   return "java.nio.ByteBuffer";
//      case INT:     return "getInt";
//      case LONG:    return "getLong";
//      case FLOAT:   return "getDouble";
//      case DOUBLE:  return "getDouble";
//      case BOOLEAN: return "getBoolean";
////      case NULL:    return "java.lang.Void";
//      default: throw new RuntimeException("Unknown type: "+schema);
//    }
//  }

  public boolean isReadOnly(Schema.Field field) {
    JsonNode readOnly = field.getJsonProp("readonly");
    return readOnly != null && readOnly.asBoolean();
  }

  public boolean isRequiredOnCreate(Schema.Field field) {
    JsonNode requiredOnCreate = field.getJsonProp("requiredOnCreate");
    return requiredOnCreate != null && requiredOnCreate.asBoolean();
  }

  public boolean isMap(Schema schema) {
    return getBaseSchema(schema).getType() == Schema.Type.MAP;
  }

  public boolean isRecord(Schema schema) {
    return getBaseSchema(schema).getType() == Schema.Type.RECORD;
  }

  public boolean isArray(Schema schema) {
    return getBaseSchema(schema).getType() == Schema.Type.ARRAY;
  }

  public boolean isEnum(Schema schema) {
    return getBaseSchema(schema).getType() == Schema.Type.ENUM;
  }

  public boolean isString(Schema schema) {
    return getBaseSchema(schema).getType() == Schema.Type.STRING;
  }

  public boolean isOptional(Schema schema) {
    if (schema.getType() == Schema.Type.UNION) {
      List<Schema> types = schema.getTypes(); // elide unions with null
      return ((types.size() == 2) && types.contains(NULL_SCHEMA));
    }
    return false;
  }

  @Override
  public String getFileHeader() {
    String defaultHeader = super.getFileHeader();
    return copyrightNotice + "\n\n" + defaultHeader;
  }

  public void setCopyrightNotice(String copyrightNotice) {
    this.copyrightNotice = copyrightNotice;
  }

  public String getBasePackage() {
    return basePackage;
  }

  public String getBaseBasePackage() {
    return basePackage.substring(0, basePackage.lastIndexOf("."));
  }

  public void setBasePackage(String basePackage) {
    this.basePackage = basePackage;
  }

  public String getNamespace(Schema schema) {
    return basePackage + "." + schema.getNamespace();
  }

  private Schema getBaseSchema(Schema schema) {
    if (schema.getType() == Schema.Type.UNION) {
      List<Schema> types = schema.getTypes(); // elide unions with null
      if ((types.size() == 2) && types.contains(NULL_SCHEMA)) {
        return types.get(types.get(0).equals(NULL_SCHEMA) ? 1 : 0);
      }
    }
    return schema;
  }

  public String javaType(Schema schema) {
    switch (schema.getType()) {
      case RECORD:
      case ENUM:
      case FIXED:
        return mangle(basePackage + "." + schema.getFullName());
      default:
        return super.javaType(schema);
    }
  }

}
