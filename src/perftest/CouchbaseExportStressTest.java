/**
 * Copyright 2011 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.cloudera.sqoop.Sqoop;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.tool.ExportTool;
import com.cloudera.sqoop.tool.SqoopTool;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import com.couchbase.sqoop.manager.CouchbaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Stress test the plugin by doing a large export into Couchbase.
 */
public class CouchbaseExportStressTest extends Configured implements Tool {

  // Export 10 GB of data. Each record is ~100 bytes.
  public static final int NUM_FILES = 10;
  public static final int RECORDS_PER_FILE = 10 * 1024 * 1024;

  public static final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

  public CouchbaseExportStressTest() {
  }

  public void createFile(int fileId) throws IOException {
    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);
    Path dirPath = new Path("CouchbaseExportStressTest");
    fs.mkdirs(dirPath);
    Path filePath = new Path(dirPath, "input-" + fileId);

    OutputStream os = fs.create(filePath);
    Writer w = new BufferedWriter(new OutputStreamWriter(os));
    for (int i = 0; i < RECORDS_PER_FILE; i++) {
      long v = (long) i + ((long) RECORDS_PER_FILE * (long) fileId);
      w.write("" + v + "," + ALPHABET + ALPHABET + ALPHABET + ALPHABET + "\n");

    }
    w.close();
    os.close();
  }

  /** Create a set of data files to export. */
  public void createData() throws IOException {
    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);
    Path dirPath = new Path("CouchbaseExportStressTest");
    if (fs.exists(dirPath)) {
      System.out.println(
          "Export directory appears to already exist. Skipping data-gen.");
      return;
    }

    for (int i = 0; i < NUM_FILES; i++) {
      createFile(i);
    }
  }

  /**
   * Actually run the export of the generated data to the user-created table.
   */
  public void runExport(String connectStr, String username) throws Exception {
    SqoopOptions options = new SqoopOptions(getConf());
    options.setConnectString(connectStr);
    options.setConnManagerClassName(CouchbaseUtils.COUCHBASE_CONN_MANAGER);
    options.setTableName("CouchbaseExportStressTestTable");
    options.setUsername(username);
    options.setExportDir("CouchbaseExportStressTest");
    options.setNumMappers(4);
    options.setLinesTerminatedBy('\n');
    options.setFieldsTerminatedBy(',');
    options.setExplicitInputDelims(true);

    SqoopTool exportTool = new ExportTool();
    Sqoop sqoop = new Sqoop(exportTool, getConf(), options);
    int ret = Sqoop.runSqoop(sqoop, new String[0]);
    if (0 != ret) {
      throw new Exception("Error doing export; ret=" + ret);
    }
  }

  @Override
  public int run(String [] args) {
    String connectStr = args[0];
    String username = args[1];

    try {
      createData();
      runExport(connectStr, username);
    } catch (Exception e) {
      System.err.println("Error: " + StringUtils.stringifyException(e));
      return 1;
    }

    return 0;
  }

  public static void main(String [] args) throws Exception {
    CouchbaseExportStressTest test = new CouchbaseExportStressTest();
    int ret = ToolRunner.run(test, args);
    System.exit(ret);
  }
}
