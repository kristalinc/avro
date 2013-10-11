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

import org.apache.avro.compiler.specific.JavaObjectCompiler;
import org.apache.avro.compiler.specific.ServerObjectCompiler;
import org.apache.avro.generic.GenericData;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Protocol;
import org.apache.avro.compiler.specific.SpecificCompiler;

// USED FOR GENERATING SERVER PROTOCOL HANDLERS
public class CloverProtocolMojo extends ProtocolMojo {
  /**
   * A set of Ant-like inclusion patterns used to select files from the source
   * directory for processing. By default, the pattern
   * <code>**&#47;*.avpr</code> is used to select grammar files.
   *
   * @parameter
   */
  private String[] includes = new String[] { "**/*.avpr" };

  /**
   * A set of Ant-like inclusion patterns used to select files from the source
   * directory for processing. By default, the pattern
   * <code>**&#47;*.avpr</code> is used to select grammar files.
   *
   * @parameter
   */
  private String[] testIncludes = new String[] { "**/*.avpr" };

  @Override
  protected void doCompile(String filename, File sourceDirectory, File outputDirectory) throws IOException {
    File src = new File(sourceDirectory, filename);

    SpecificCompiler compiler = null;

    if (!filename.endsWith(".avpr")) {
      return;
    }

    Protocol protocol = Protocol.parse(src);
    compiler = new JavaObjectCompiler(protocol);


    // Data object compiler
    compiler.setTemplateDir("/com/clover/avro/templates/java/core/");
    compiler.setStringType(GenericData.StringType.String);
    //compiler.setFieldVisibility(getFieldVisibility());
    compiler.setCreateSetters(createSetters);
    compiler.compileToDestination(src, outputDirectory);

    //ServerCompiler
    ServerObjectCompiler sCompiler = new ServerObjectCompiler(protocol);
    sCompiler.setTemplateDir("/com/clover/avro/templates/java/server/");
    sCompiler.compileToDestination(src, new File(outputDirectory, "server"));
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
