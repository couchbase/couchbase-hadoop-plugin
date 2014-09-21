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

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.TestExport;
import com.couchbase.client.CouchbaseClient;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that the plugin export functionality works correctly.
 */
public class CouchbaseExportTest extends TestExport {

  private CouchbaseManager manager;

  private CouchbaseClient cb;

  @Override
  protected Connection getConnection() {
    return null;
  }

  @Override
  protected boolean useHsqldbTestServer() {
    return true;
  }

  @Override
  protected String getConnectString() {
    return CouchbaseDefaultBucketUtils.CONNECT_STRING;
  }

  @Before
  public void setUp() {
    getConf().set("connection.manager", CouchbaseDefaultBucketUtils.COUCHBASE_CONN_MANAGER);
    super.setUp();

    SqoopOptions options = new SqoopOptions(CouchbaseDefaultBucketUtils.CONNECT_STRING,
        getTableName());
    options.setConnManagerClassName(CouchbaseDefaultBucketUtils.COUCHBASE_CONN_MANAGER);
    this.manager = new CouchbaseManager(options);

    String connStr = CouchbaseDefaultBucketUtils.CONNECT_STRING;
    String user = CouchbaseDefaultBucketUtils.COUCHBASE_USER_NAME;
    String pass = CouchbaseDefaultBucketUtils.COUCHBASE_USER_PASS;

    try {
      cb = new CouchbaseClient(Arrays.asList(new URI(connStr)), user, user,
          pass);
    } catch (IOException e) {
      fail("Couldn't connect to Couchbase Server");
    } catch (URISyntaxException e) {
      fail("Invalid URI for Couchbase Server");
    }
  }

  @After
  public void tearDown() {
    super.tearDown();
    cb.shutdown();

    if (null != manager) {
      try {
        manager.close();
        manager = null;
      } catch (SQLException sqlE) {
        LOG.error("Got SQLException: " + sqlE.toString());
        fail("Got SQLException: " + sqlE.toString());
      }
    }
  }

  @Override
  protected String [] getCodeGenArgv(String... extraArgs) {
    String [] moreArgs = new String[extraArgs.length + 6];
    int i = 0;

    for (i = 0; i < extraArgs.length; i++) {
      moreArgs[i] = extraArgs[i];
    }

    // Add username and password args.
    moreArgs[i++] = "--username";
    moreArgs[i++] = CouchbaseDefaultBucketUtils.COUCHBASE_USER_NAME;
    moreArgs[i++] = "--password";
    moreArgs[i++] = CouchbaseDefaultBucketUtils.COUCHBASE_USER_PASS;
    moreArgs[i++] = "--connection-manager";
    moreArgs[i++] = CouchbaseDefaultBucketUtils.COUCHBASE_CONN_MANAGER;

    return super.getCodeGenArgv(moreArgs);
  }

  @Override
  protected String [] getArgv(boolean includeHadoopFlags,
    int rowsPerStatement, int statementsPerTx, String... additionalArgv) {

    String [] subArgv = newStrArray(additionalArgv,
        "--username", CouchbaseDefaultBucketUtils.COUCHBASE_USER_NAME,
        "--password", CouchbaseDefaultBucketUtils.COUCHBASE_USER_PASS,
        "--connection-manager", CouchbaseDefaultBucketUtils.COUCHBASE_CONN_MANAGER);
    return super.getArgv(includeHadoopFlags, rowsPerStatement,
        statementsPerTx, subArgv);
  }

  @Override
  public void createTable(ColumnGenerator... extraColumns) throws SQLException {
    // Couchbase doesn't use tables.
  }

  @Override
  protected void verifyExport(int expectedNumRecords)
    throws IOException, SQLException {
    try {
      System.err.println("Going to get " + expectedNumRecords);
      for (int i = 0; i < expectedNumRecords; i++) {
        String toGet = Integer.toString(i);
        if (cb.get(toGet) == null) {
          fail("Failed to get an exported key from Couchbase.  Could not get "
            + toGet + " from connection " + cb.getNodeLocator().getPrimary(toGet));
        }
      }
      if (!cb.flush().get().booleanValue()) {
        fail("Unable to flush keys.  The flush_all command is disabled by "
          + "default from Couchbase Server version 1.8.1 and on.  Re-enable it"
          + " for testing.");
      }
    } catch (InterruptedException e) {
      fail("Interrupted while checking keys:" + e.getMessage());
    } catch (ExecutionException e) {
      fail("Operation failed while checking keys:" + e.getMessage());
    }
  }

  @Test
  @Override
  public void testNumericTypes() throws IOException, SQLException {
    // Couchbase only reads strings.
  }

  @Test
  @Override
  public void testDatesAndTimes() throws IOException, SQLException {
    // Couchbase only reads strings.
  }

  @Test
  @Override
  public void testBigIntCol() throws IOException, SQLException {
    // Couchbase only has two columns.
  }

  @Test
  @Override
  public void testIntCol() throws IOException, SQLException {
    // Couchbase only has two columns.
  }

  @Test
  @Override
  public void testMultiTransactionWithStaging() {
    // Couchbase doesn't support staging tables
  }

  @Test
  @Override
  public void testMultiMapTextExportWithStaging() {
    // Couchbase doesn't support staging tables
  }

  @Test
  @Override
  public void testColumnsExport() throws IOException, SQLException {
    // Couchbase only has two columns.
  }
}
