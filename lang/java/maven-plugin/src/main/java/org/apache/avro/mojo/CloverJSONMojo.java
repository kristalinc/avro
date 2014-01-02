/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro.mojo;

import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.JSONObjectCompiler;
import org.apache.avro.generic.GenericData.StringType;

import java.io.File;
import java.io.IOException;

/**
 * Generate Java classes from Avro schema files (.avsc)
 *
 * @goal clover-json
 * @phase generate-sources
 * @threadSafe
 */
public class CloverJSONMojo extends AbstractAvroMojo {
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

  private static final String COPYRIGHT_NOTICE = "/*\n" +
      " * Copyright (C) 2013 Clover Network, Inc.\n" +
      " *\n" +
      " * Licensed under the Apache License, Version 2.0 (the \"License\");\n" +
      " * you may not use this file except in compliance with the License.\n" +
      " * You may obtain a copy of the License at\n" +
      " *\n" +
      " *    http://www.apache.org/licenses/LICENSE-2.0\n" +
      " *\n" +
      " * Unless required by applicable law or agreed to in writing, software\n" +
      " * distributed under the License is distributed on an \"AS IS\" BASIS,\n" +
      " * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n" +
      " * See the License for the specific language governing permissions and\n" +
      " * limitations under the License.\n" +
      " */";

  @Override
  protected void doCompile(String filename, File sourceDirectory, File outputDirectory) throws IOException {
    File src = new File(sourceDirectory, filename);
    Schema schema;

    // This is necessary to maintain backward-compatibility. If there are  
    // no imported files then isolate the schemas from each other, otherwise
    // allow them to share a single schema so reuse and sharing of schema
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

    JSONObjectCompiler compiler = new JSONObjectCompiler(schema);
    compiler.setCopyrightNotice(COPYRIGHT_NOTICE);
    compiler.setTemplateDir("/com/clover/avro/templates/jsonobject/");
    compiler.setStringType(StringType.String);
    compiler.setBasePackage(basePackage);
    //compiler.setFieldVisibility(getFieldVisibility());
    compiler.setCreateSetters(createSetters);
    compiler.compileToDestination(src, outputDirectory);
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
