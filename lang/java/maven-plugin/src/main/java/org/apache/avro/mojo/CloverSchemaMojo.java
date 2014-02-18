package org.apache.avro.mojo;

import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.JavaObjectCompiler;
import org.apache.avro.generic.GenericData;

import java.io.File;
import java.io.IOException;

/**
 * Generate Java classes from Avro schema files (.avsc)
 *
 * @goal clover-schema
 * @phase generate-sources
 * @threadSafe
 */
public class CloverSchemaMojo extends AbstractAvroMojo {
  /**
   * A parser used to parse all schema files. Using a common parser will
   * facilitate the import of external schemas.
   */
  private Schema.Parser schemaParser = new Schema.Parser();

  /**
   * A set of Ant-like inclusion patterns used to select files from the source
   * directory for processing. By default, the pattern
   * <code>**&#47;*.avsc</code> is used to select grammar files.
   *
   * @parameter
   */
  private String[] includes = new String[] { "**/*.avsc" };

  /**
   * A set of Ant-like inclusion patterns used to select files from the source
   * directory for processing. By default, the pattern
   * <code>**&#47;*.avsc</code> is used to select grammar files.
   *
   * @parameter
   */
  private String[] testIncludes = new String[] { "**/*.avsc" };

  /**
   * @parameter
   */
  private String basePackage = "";

  @Override
  protected void doCompile(String filename, File sourceDirectory, File outputDirectory) throws IOException {
    File src = new File(sourceDirectory, filename);
    Schema schema;

    // This is necessary to maintain backward-compatibility. If there are
    // no imported files then isolate the schemas from each other, otherwise
    // allow them to share a single schema so resuse and sharing of schema
    // is possible.
    if (imports == null) {
      schema = new Schema.Parser().parse(src);
    } else {
      schema = schemaParser.parse(src);
    }

    if (basePackage != null) {
      String packageDir = basePackage.replace('.', File.separatorChar);
      outputDirectory = new File(outputDirectory, packageDir);
    }

    JavaObjectCompiler compiler = new JavaObjectCompiler(schema);
    compiler.setTemplateDir("/com/clover/avro/templates/java/core/");
    //compiler.setTemplateDir(templateDirectory);
    //compiler.setStringType(GenericData.StringType.valueOf(stringType));
    //compiler.setFieldVisibility(getFieldVisibility());
    compiler.setBasePackage(basePackage);
    compiler.setStringType(GenericData.StringType.String);
    compiler.setCreateSetters(createSetters);
    compiler.compileToDestination(src, outputDirectory);
  }

  public String getBasePackage() {
    return basePackage;
  }

  public void setBasePackage(String basePackage) {
    this.basePackage = basePackage;
  }

  @Override
  protected String[] getIncludes() {
    return includes;
  }

  @Override
  protected String[] getTestIncludes() {
    return testIncludes;
  }
}

