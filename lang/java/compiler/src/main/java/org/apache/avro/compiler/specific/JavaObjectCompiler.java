package org.apache.avro.compiler.specific;

import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;

/**
 * Created with IntelliJ IDEA.
 * User: ddmoore
 * Date: 8/28/13
 * Time: 9:56 AM
 */
public class JavaObjectCompiler extends SpecificCompiler {
  private static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);

  public JavaObjectCompiler(Schema schema) {
    super(schema);
  }

  public String generateValidityCheck(Schema.Field field, String fieldName) {
    String check = "";
    Schema schema = field.schema();
    switch (schema.getType()) {
      case RECORD:
      case ENUM:
      case FIXED:
        break;
      case ARRAY:
        break;
//      case MAP:
//        return "java.util.Map<"
//                + getStringType(schema.getJsonProp(SpecificData.KEY_CLASS_PROP))+","
//                + javaType(schema.getValueType()) + ">";
      case UNION:
        return "";
      case STRING:
        JsonNode length = field.getJsonProp("length");
        if (length != null) {
          check = "if (" + fieldName + " != null && " + fieldName + ".length() > " + length.asLong() + ") throw new IllegalArgumentException(\"Maximum string length exceeded for '" + fieldName + "'\");";
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

  // TODO: figure out what to do with maps
  public boolean isArray(Schema schema) {
    return schema.getType() == Schema.Type.ARRAY;
  }

  public boolean isEnum(Schema schema) {
    return schema.getType() == Schema.Type.ENUM;
  }

  public boolean isString(Schema schema) {
    return schema.getType() == Schema.Type.STRING;
  }
}
