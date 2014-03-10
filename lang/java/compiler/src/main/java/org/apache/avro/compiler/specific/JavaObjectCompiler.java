package org.apache.avro.compiler.specific;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: ddmoore
 * Date: 8/28/13
 * Time: 9:56 AM
 */
public class JavaObjectCompiler extends SpecificCompiler {
  private static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);
  private String basePackage;

  public JavaObjectCompiler(Schema schema) {
    super(schema);
  }

  public JavaObjectCompiler(Protocol protocol) {
    super(protocol);
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
          check = "if (" + fieldName + " != null && " + fieldName + ".length() > " + length.asLong() + ") errors.add(\"Maximum string length exceeded for '" + fieldName + "'. Max length is " + length.asLong() + "\");";
        }
        break;
      case BYTES:   break;
      case INT:     break;
      case LONG:    break;
      case FLOAT:   break;
      case DOUBLE:  break;
      case BOOLEAN: break;
      case NULL:    break;
      default: throw new RuntimeException("Unknown type: "+schema);
    }
    return check;
  }

  public String getBasePackage() {
    return basePackage;
  }

  public void setBasePackage(String basePackage) {
    this.basePackage = basePackage;
  }

  public String getNamespace(Schema schema) {
    return basePackage + "." + schema.getNamespace();
  }

  public List<Schema.Field> getRequiredFields(Schema schema) {
    List<Schema.Field> requiredFields = new ArrayList<Schema.Field>();
    for (Schema.Field field : schema.getFields()) {
      if (isRequiredOnCreate(field)) requiredFields.add(field);
    }
    return requiredFields;
  }

  public boolean isRequiredOnCreate(Schema.Field field) {
    JsonNode requiredOnCreate = field.getJsonProp("requiredOnCreate");
    return requiredOnCreate != null && requiredOnCreate.asBoolean();
  }

  public static String mangle(String name) {
    if ("id".equals(name)) {
      return "uuid";
    }
    return SpecificCompiler.mangle(name);
  }

  public static String mangle(String name, boolean ignore) {
    return mangle(name);
  }

  // TODO: figure out what to do with maps
  public boolean isArray(Schema.Field field) {
    return field.schema().getType() == Schema.Type.ARRAY;
  }

  /** Utility for template use.  Returns the java type for a Schema. */
  public String javaType(Schema schema) {
    switch (schema.getType()) {
      case RECORD:
      case ENUM:
      case FIXED:
        return mangle("com.clover.server.data." + schema.getFullName());
      default:
        return super.javaType(schema);
    }
  }

  public static String parseUrl(String url) {
    return url.replace("{id}", "\" + getUuid() + \"");
  }

  public String writeFieldEncoder(Schema.Field field) {
    String url = field.getProp("url");

    if (url != null) {
      if (isArray(field)) {
        return "encoder.writeMerchantRelativeList(\"" + field.name() + "\", " + mangle(field.name())
               + ", \"" + parseUrl(url) + "\");";
      } else {
        return "encoder.writeMerchantRelativeReference(\"" + field.name() + "\", " + mangle(field.name())
               + ", \"" + parseUrl(url) + "\");";
      }
    } else {
      if (isArray(field)) {
        return "encoder.writeList(\"" + field.name() + "\", " + mangle(field.name()) + ");";
      } else {
        return  "encoder.write(\"" + field.name() + "\", " + mangle(field.name()) + ");";
      }
    }
  }

  public boolean isMap(Schema.Field field) {
    return field.schema().getType() == Schema.Type.MAP;
  }

  public boolean isEnum(Schema schema) {
    return schema.getType() == Schema.Type.ENUM;
  }

  public boolean isString(Schema schema) {
    return schema.getType() == Schema.Type.STRING;
  }

  public boolean isRecord(Schema.Field field) {
    return field.schema().getType() == Schema.Type.RECORD;
  }

  public boolean isReference(Schema.Field field) {
    return field.schema().getName().equals("Reference");
  }

  public String generateEnum(List<Schema.Field> fields) {
    String fieldList = null;
    for (Schema.Field field: fields) {
      if (fieldList == null) {
        fieldList = "";
      } else {
        fieldList += ", ";
      }
      fieldList += camelCaseToUnderscore(field.name()).toUpperCase();
    }
    return fieldList;
  }
  public String generateFieldEnum(Schema schema) {
    return generateEnum(schema.getFields());
  }

  public boolean hasForeignRef(Schema schema) {
    return schema.getJsonProp("foreign_references") != null;
  }
  public String generateTableEnum(Schema schema) {
    JsonNode references = schema.getJsonProp("foreign_references");
    if (references == null) {return null;}
    Iterator<Map.Entry<String, JsonNode>> it = references.getFields ();
    String fieldList = null;
    while (it.hasNext()) {
      if (fieldList == null) {
        fieldList = "";
      } else {
        fieldList += ", ";
      }
      fieldList += camelCaseToUnderscore(it.next().getKey()).toUpperCase();
    }
    return fieldList;
  }

  public List<Map.Entry<String, JsonNode>> getTablePairs(Schema schema) {
    JsonNode references = schema.getJsonProp("foreign_references");
    if (references == null) {return null;}
    List<Map.Entry<String, JsonNode>> entries = new ArrayList<Map.Entry<String, JsonNode>>();
    Iterator<Map.Entry<String, JsonNode>> it = references.getFields ();
    while (it.hasNext()) {
      entries.add(it.next());
    }
    return entries;
  }

  public String getTable(Schema.Field field) {
    JsonNode sql = field.getJsonProp("sql");
    if (sql != null) {
      JsonNode table = sql.get("table");
      return table != null ? table.asText().toUpperCase() : null;
    }
    return null;
  }

  public String camelCaseToUnderscore(String input) {
    String regex = "(.)([A-Z])";
    String replacement = "$1_$2";
    return input.replaceAll(regex,replacement).toLowerCase();
  }

  public String renamedFrom(Schema.Field field) {
    return field.getProp("renamed_from");
  }

  public String getSqlName(Schema.Field field) {
    JsonNode sql = field.getJsonProp("sql");
    if (sql != null) {
      JsonNode name = sql.get("name");
      return name != null ? name.asText() : camelCaseToUnderscore(field.name());
    }
    return camelCaseToUnderscore(field.name()).toLowerCase();
  }



  public static String generateGetMethod(Schema schema, Schema.Field field) {
    if ("id".equalsIgnoreCase(field.name())) {
      return "getUuid";
    }
    return SpecificCompiler.generateGetMethod(schema, field);
  }

  public String generateSetMethod(Schema schema, String object) {
    return "set" + Character.toUpperCase(object.charAt(0)) + object.substring(1);
  }

  public String generateBuilderMethod(Schema schema, Schema.Field field) {
    String setMethodName = generateSetMethod(schema, field);
    String baseMethodName = setMethodName.substring(3);
    if (baseMethodName.equalsIgnoreCase("id")) {
      return "uuid";
    }
    return Character.toLowerCase(baseMethodName.charAt(0)) + baseMethodName.substring(1);
  }


  public boolean isReadOnly(Schema.Field field) {
    JsonNode readOnly = field.getJsonProp("readonly");
    return readOnly != null && readOnly.asBoolean();
  }
  public List<Schema.Field> getSettableFields(Schema schema) {
    List<Schema.Field> settableFields = new ArrayList<Schema.Field>();
    for (Schema.Field field : schema.getFields()) {
      if (!isReadOnly(field)) settableFields.add(field);
    }
    return settableFields;
  }
}
