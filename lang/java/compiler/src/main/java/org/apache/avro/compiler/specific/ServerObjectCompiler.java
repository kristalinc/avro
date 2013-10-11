package org.apache.avro.compiler.specific;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

/**
 * User: josh
 * Date: 9/27/13
 */
public class ServerObjectCompiler {
  private Protocol protocol;
  private VelocityEngine velocityEngine;

  private String templateDir;
  private String protocolPackage;
  private static String logChuteName = null;
  private static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);


  public Protocol getProtocol() {
    return protocol;
  }

  public String nameToUpper(String name) {
    return Character.toUpperCase(name.charAt(0)) + name.substring(1);
  }

  public String nameToLower(String name) {
    return Character.toLowerCase(name.charAt(0)) + name.substring(1);
  }

  ServerObjectCompiler() {
    initializeVelocity();
  }

  public ServerObjectCompiler(Protocol protocol) {
    this();
    this.protocol = protocol;
  }

  public void setTemplateDir(String templateDir) {
    this.templateDir = templateDir;
  }

  /** Generate output under dst, unless existing file is newer than src. */
  public void compileToDestination(File src, File dst) throws IOException {
    for (Map.Entry<String, Protocol.Message> s : protocol.getMessages().entrySet()) {
      SpecificCompiler.OutputFile o = compile(s.getValue());
      o.writeToDestination(src, dst);
    }
  }


  private void initializeVelocity() {
    this.velocityEngine = new VelocityEngine();

    // These  properties tell Velocity to use its own classpath-based
    // loader, then drop down to check the root and the current folder
    velocityEngine.addProperty("resource.loader", "class, file");
    velocityEngine.addProperty("class.resource.loader.class",
            "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
    velocityEngine.addProperty("file.resource.loader.class",
            "org.apache.velocity.runtime.resource.loader.FileResourceLoader");
    velocityEngine.addProperty("file.resource.loader.path", "/, .");
    velocityEngine.setProperty("runtime.references.strict", true);

    // try to use Slf4jLogChute, but if we can't use the null one.
    if (null == logChuteName) {
      // multiple threads can get here concurrently, but that's ok.
      try {
        new SpecificCompiler.Slf4jLogChute();
        logChuteName = SpecificCompiler.Slf4jLogChute.class.getName();
      } catch (Exception e) {
        logChuteName = "org.apache.velocity.runtime.log.NullLogChute";
      }
    }
    velocityEngine.setProperty("runtime.log.logsystem.class", logChuteName);
  }

  public String getNamespace() {
    return protocol.getNamespace();
  }

  SpecificCompiler.OutputFile compile(Protocol.Message message) {
    VelocityContext context = new VelocityContext();
    context.put("this", this);
    context.put("message", message);
    String output = renderTemplate(templateDir+"handler.vm", context);


    SpecificCompiler.OutputFile outputFile = new  SpecificCompiler.OutputFile();

    String name = nameToUpper(message.getName()) + "Handler";
    outputFile.path = SpecificCompiler.makePath(name, "com.clover.server.handlers.api." + protocol.getNamespace());
    outputFile.contents = output;
    return outputFile;
  }

  private String renderTemplate(String templateName, VelocityContext context) {
    Template template;
    try {
      template = this.velocityEngine.getTemplate(templateName);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    StringWriter writer = new StringWriter();
    template.merge(context, writer);
    return writer.toString();
  }

  /** Utility for template use.  Returns the java type for a Schema. */
  public String javaType(Schema schema) {
    switch (schema.getType()) {
      case RECORD:
      case ENUM:
      case FIXED:
        return "com.clover.core.data." + schema.getFullName();
      case ARRAY:
        return "java.util.List<" + javaType(schema.getElementType()) + ">";
      case MAP:
        return "java.util.Map<"
                + "java.lang.String,"
                + javaType(schema.getValueType()) + ">";
      case UNION:
        List<Schema> types = schema.getTypes(); // elide unions with null
        if ((types.size() == 2) && types.contains(NULL_SCHEMA))
          return javaType(types.get(types.get(0).equals(NULL_SCHEMA) ? 1 : 0));
        return "java.lang.Object";
      case STRING:  return "java.lang.String";
      case BYTES:   return "java.nio.ByteBuffer";
      case INT:     return "java.lang.Integer";
      case LONG:    return "java.lang.Long";
      case FLOAT:   return "java.lang.Float";
      case DOUBLE:  return "java.lang.Double";
      case BOOLEAN: return "java.lang.Boolean";
      case NULL:    return "java.lang.Void";
      default: throw new RuntimeException("Unknown type: "+schema);
    }
  }
}
