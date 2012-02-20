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
package com.couchbase.sqoop.lib;

import net.spy.memcached.CachedData;
import net.spy.memcached.tapmessage.ResponseMessage;
import net.spy.memcached.transcoders.SerializingTranscoder;

/**
 * Couchbase Hadoop plugin utility functions.
 */
public final class CouchbaseRecordUtil {

  private CouchbaseRecordUtil() {};

  /**
   * Attempt to get the object represented by the given serialized bytes.
   */
  public static Object deserialize(ResponseMessage message) {
    SerializingTranscoder tc = new SerializingTranscoder();
    CachedData d = new CachedData(message.getItemFlags(), message.getValue(),
      CachedData.MAX_SIZE);
    Object rv = null;
    rv = tc.decode(d);
    return rv;
  }

}
