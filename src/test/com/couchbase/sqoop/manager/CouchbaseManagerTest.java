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

package com.couchbase.sqoop.manager;

import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import com.cloudera.sqoop.util.FileListing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import net.spy.memcached.MembaseClient;
import net.spy.memcached.MemcachedClient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the import functionality of the plugin.
 */
public class CouchbaseManagerTest extends ImportJobTestCase {
  public static final Log LOG = LogFactory.getLog(
      CouchbaseManagerTest.class.getName());

  private static final String TABLE_NAME = "DUMP";
  private static final int NUM_RECORDS = 5;

  private MemcachedClient mc;

  @Before
  public void setUp() {
    super.setUp();

    try {
      URI uri = new URI(CouchbaseUtils.CONNECT_STRING);
      String user = CouchbaseUtils.COUCHBASE_USER_NAME;
      String pass = CouchbaseUtils.COUCHBASE_USER_PASS;
      mc = new MembaseClient(Arrays.asList(uri), user, user, pass);
    } catch (URISyntaxException e) {
      LOG.error("Bad URL" + e.getMessage());
      fail(e.toString());
    } catch (IOException e) {
      LOG.error("Couldn't connect to server" + e.getMessage());
      fail(e.toString());
    }
  }

  @After
  public void tearDown() {
    mc.shutdown();
  }

  private String[] getArgv() {
    ArrayList<String> args = new ArrayList<String>();

    CommonArgs.addHadoopFlags(args);

    args.add("--connection-manager");
    args.add(CouchbaseUtils.COUCHBASE_CONN_MANAGER);
    args.add("--table");
    args.add(TABLE_NAME);
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(CouchbaseUtils.CONNECT_STRING);
    args.add("--username");
    args.add(CouchbaseUtils.COUCHBASE_USER_NAME);
    args.add("--password");
    args.add(CouchbaseUtils.COUCHBASE_USER_PASS);
    args.add("--num-mappers");
    args.add("2");

    return args.toArray(new String[0]);
  }

  @Test
  public void testCouchbaseImport() throws IOException {
    HashMap<String, String> expectedResults = new HashMap<String, String>();
    for (int i = 0; i < NUM_RECORDS; i++) {
      String kv = i + "";
      expectedResults.put(kv, kv);
      mc.set(kv, 0, kv);
    }

    runCouchbaseTest(expectedResults);
  }

  private void runCouchbaseTest(HashMap<String, String> expectedMap)
    throws IOException {
    Path warehousePath = new Path(this.getWarehouseDir());
    Path tablePath = new Path(warehousePath, TABLE_NAME);
    Path filePath = new Path(tablePath, "part-m-00000");

    File tableFile = new File(tablePath.toString());
    if (tableFile.exists() && tableFile.isDirectory()) {
      // remove the directory before running the import.
      FileListing.recursiveDeleteDir(tableFile);
    }

    String[] argv = getArgv();
    try {
      runImport(argv);
    } catch (IOException ioe) {
      LOG.error("Got IOException during import: " + ioe.toString());
      ioe.printStackTrace();
      fail(ioe.toString());
    }

    File f = new File(filePath.toString());
    assertTrue("Could not find imported data file", f.exists());
    BufferedReader r = null;
    try {
      // Read through the file and make sure it's all there.
      r = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
      String line;
      int records = 0;
      while ((line = r.readLine()) != null) {
        compareRecords(expectedMap, line);
        records++;
      }
      if (records < NUM_RECORDS) {
        fail("Not everything was imported. Got " + records + "/" + NUM_RECORDS
            + " records.");
      }
    } catch (IOException ioe) {
      LOG.error("Got IOException verifying results: " + ioe.toString());
      ioe.printStackTrace();
      fail(ioe.toString());
    } finally {
      IOUtils.closeStream(r);
    }
  }

  /**
   * Compare two lines. Normalize the dates we receive based on the expected
   * time zone.
   *
   * @param expectedLine
   *            expected line
   * @param receivedLine
   *            received line
   * @throws IOException
   *             exception during lines comparison
   */
  private void compareRecords(HashMap<String, String> expectedMap,
      String receivedLine)
    throws IOException {
    String[] receivedValues = receivedLine.split(",");
    String key = receivedValues[0];
    String value = receivedValues[1];

    String expectedValue;
    if ((expectedValue = expectedMap.get(key)) != null) {
      if (expectedValue.equals(value)) {
        return;
      }
    }
    fail("Couldn't find expected key " + key);
  }
}
