package org.apache.avro.tool;

import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.JSONObjectCompiler;
import org.apache.avro.compiler.specific.JavaObjectCompiler;
import org.apache.avro.generic.GenericData;

import java.io.File;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: ddmoore
 * Date: 8/25/13
 * Time: 12:15 PM
 */
public class CloverSDKTool implements Tool {
  @Override
  public int run(InputStream in, PrintStream out, PrintStream err,
                 List<String> args) throws Exception {
    if (args.size() < 3) {
      System.err.println("Usage: (json|java) input... outputdir");
      System.err.println(" input - input files or directories");
      System.err.println(" outputdir - directory to write generated java");
      return 1;
    }

    GenericData.StringType stringType = GenericData.StringType.String;

    int arg = 0;

    String method = args.get(arg);
    List<File> inputs = new ArrayList<File>();
    File output = new File(args.get(args.size() - 1));

    for (int i = arg + 1; i < args.size() - 1; i++) {
      inputs.add(new File(args.get(i)));
    }

    if ("json".equals(method)) {
      // set this property to use our specific velocity templates rather than the Avro default templates
      System.setProperty("org.apache.avro.specific.templates", "/com/clover/avro/templates/jsonobject/");
      Schema.Parser parser = new Schema.Parser();
      for (File src : determineInputs(inputs, SCHEMA_FILTER)) {
        Schema schema = parser.parse(src);
        JSONObjectCompiler compiler = new JSONObjectCompiler(schema);
        compiler.setCreateSetters(true); // do we always want to do this?
        compiler.setStringType(stringType);
        compiler.compileToDestination(src, output);
      }
    } else if ("java".equals(method)) {
        // set this property to use our specific velocity templates rather than the Avro default templates
        System.setProperty("org.apache.avro.specific.templates", "/com/clover/avro/templates/java/");
        Schema.Parser parser = new Schema.Parser();
        for (File src : determineInputs(inputs, SCHEMA_FILTER)) {
          Schema schema = parser.parse(src);
          JavaObjectCompiler compiler = new JavaObjectCompiler(schema);
          compiler.setCreateSetters(true); // do we always want to do this?
          compiler.setStringType(stringType);
          compiler.compileToDestination(src, output);
        }
    } else {
      System.err.println("Expected \"json\" or \"java\".");
      return 1;
    }

    return 0;
  }

  @Override
  public String getName() {
    return "cloversdk";
  }

  @Override
  public String getShortDescription() {
    return "Generates code from the given schema for Clover SDK";
  }

  /**
   * For a List of files or directories, returns a File[] containing each file
   * passed as well as each file with a matching extension found in the directory.
   *
   * @param inputs List of File objects that are files or directories
   * @param filter File extension filter to match on when fetching files from a directory
   * @return Unique array of files
   */
  private static File[] determineInputs(List<File> inputs, FilenameFilter filter) {
    Set<File> fileSet = new LinkedHashSet<File>(); // preserve order and uniqueness

    for (File file : inputs) {
      // if directory, look at contents to see what files match extension
      if (file.isDirectory()) {
        for (File f : file.listFiles(filter)) {
          fileSet.add(f);
        }
      }
      // otherwise, just add the file.
      else {
        fileSet.add(file);
      }
    }

    if (fileSet.size() > 0) {
      System.err.println("Input files to compile:");
      for (File file : fileSet) {
        System.err.println("  " + file);
      }
    } else {
      System.err.println("No input files found.");
    }

    return fileSet.toArray((new File[fileSet.size()]));
  }

  private static final FileExtensionFilter SCHEMA_FILTER =
          new FileExtensionFilter("avsc");

  private static class FileExtensionFilter implements FilenameFilter {
    private String extension;

    private FileExtensionFilter(String extension) {
      this.extension = extension;
    }

    @Override
    public boolean accept(File dir, String name) {
      return name.endsWith(this.extension);
    }
  }
}
