/**
 * Copyright 2012 Couchbase, Inc.
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
package com.couchbase.sqoop.mapreduce.db;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.TapClient;
import com.couchbase.sqoop.lib.CouchbaseRecordUtil;
import com.couchbase.sqoop.manager.CouchbaseUtils;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import junit.framework.TestCase;
import net.spy.memcached.tapmessage.ResponseMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CouchbaseRecordReadSerializeTest extends TestCase {

      public static final Log LOG = LogFactory.getLog(
      CouchbaseRecordReadSerializeTest.class.getName());
      private CouchbaseClient cb;

      private TapClient client;

      private Map<String, ResponseMessage> tappedStuff;

      private Date rightnow;
      private String dateText;

  @Before
    @Override
  public void setUp() throws Exception {
    super.setUp();

    tappedStuff = new HashMap<String, ResponseMessage>();

      URI uri = new URI(CouchbaseUtils.CONNECT_STRING);
      String user = CouchbaseUtils.COUCHBASE_USER_NAME;
      String pass = CouchbaseUtils.COUCHBASE_USER_PASS;

    try {
      cb = new CouchbaseClient(Arrays.asList(uri), user, pass);
    } catch (IOException e) {
      LOG.error("Couldn't connect to server" + e.getMessage());
      fail(e.toString());
    }
      this.client = new TapClient(Arrays.asList(uri), user, pass);

      cb.flush();
      Thread.sleep(500);


    // set up the items we're going to deserialize
    Integer anint = new Integer(Integer.MIN_VALUE);
    cb.set(anint.toString(), 0x300, anint).get();

    Long along = new Long(Long.MAX_VALUE);
    cb.set(along.toString(), 0, along).get();

    Float afloat = new Float(Float.MAX_VALUE);
    cb.set(afloat.toString(), 0, afloat).get();

    Double DOUBLEBASE = new Double(Double.NEGATIVE_INFINITY);
    cb.set(DOUBLEBASE.toString(), 0, DOUBLEBASE).get();

    Boolean BOOLEANBASE = true;
    cb.set(BOOLEANBASE.toString(), 0, BOOLEANBASE).get();

    rightnow = new Date(); // instance, needed later
    dateText = rightnow.toString().replaceAll(" ", "_");
    cb.set(dateText, 0, rightnow).get();

    Byte byteMeSix = new Byte("6");
    cb.set(byteMeSix.toString(), 0, byteMeSix).get();

    String ourString = "hi,there";
    cb.set(ourString.toString(), 0, ourString).get();

    client.tapDump("tester");
    while (client.hasMoreMessages()) {
        ResponseMessage m = client.getNextMessage();
        if (m == null) {
            continue;
        }
        tappedStuff.put(m.getKey(), m);
    }
  }

  @After
    @Override
  public void tearDown() {
    cb.shutdown();
    client.shutdown();
  }

    @Test
    public void testDeserializer() {
        for (Map.Entry<String, ResponseMessage> entry : tappedStuff.entrySet()) {
            if (entry.getKey().matches(dateText)) {
                assertEquals("Could not verify", dateText,
                  (CouchbaseRecordUtil.deserialize(entry.getValue()).toString().replaceAll(" ", "_")));
            } else {
            assertEquals("Could not verify ", entry.getKey(),
              (CouchbaseRecordUtil.deserialize(entry.getValue()).toString()));
            }
        }
    }


}
