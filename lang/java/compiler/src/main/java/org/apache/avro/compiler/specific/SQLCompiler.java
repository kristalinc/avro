package org.apache.avro.compiler.specific;

import org.apache.avro.Schema;

/**
 * Created with IntelliJ IDEA.
 * User: ddmoore
 * Date: 8/27/13
 * Time: 3:05 PM
 */
public class SQLCompiler extends SpecificCompiler {
  public String getIdentifier(Schema schema) {
    String regex = "([a-z])([A-Z])";
    String replacement = "$1_$2";
    System.out.println("CamelCaseToSomethingElse".replaceAll(regex, replacement));
    return null;
  }
}
