/**
 * Copyright 2011-2012 Couchbase, Inc.
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
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.DefaultManagerFactory;
import com.cloudera.sqoop.manager.ManagerFactory;
import com.cloudera.sqoop.metastore.JobData;

import java.lang.reflect.Constructor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Contains instantiation code for the CouchbaseManager implementation
 * in Sqoop.
 */
public class CouchbaseFactory extends ManagerFactory {

  public static final Log LOG = LogFactory.getLog(
      DefaultManagerFactory.class.getName());

  @Override
  public ConnManager accept(JobData data) {
    SqoopOptions options = data.getSqoopOptions();

    if (null != options.getConnManagerClassName()){
      String className = options.getConnManagerClassName();

      ConnManager connManager = null;
      try {
        Class<ConnManager> cls = (Class<ConnManager>) Class.forName(className);
        Constructor<ConnManager> constructor =
          cls.getDeclaredConstructor(SqoopOptions.class);
        connManager = constructor.newInstance(options);
      } catch (Exception e) {
        System.err
          .println("problem finding the connection manager for class name :"
            + className);
        // Log the stack trace for this exception
        LOG.debug(e.getMessage(), e);
        // Print exception message.
        System.err.println(e.getMessage());
      }
      return connManager;
    }

    String connectStr = options.getConnectString();

    // java.net.URL follows RFC-2396 literally, which does not allow a ':'
    // character in the scheme component (section 3.1). JDBC connect strings,
    // however, commonly have a multi-scheme addressing system. e.g.,
    // jdbc:mysql://...; so we cannot parse the scheme component via URL
    // objects. Instead, attempt to pull out the scheme as best as we can.

    // First, see if this is of the form [scheme://hostname-and-etc..]
    int schemeStopIdx = connectStr.indexOf("//");
    if (-1 == schemeStopIdx) {
      // If no hostname start marker ("//"), then look for the right-most ':'
      // character.
      schemeStopIdx = connectStr.lastIndexOf(':');
      if (-1 == schemeStopIdx) {
        // Warn that this is nonstandard. But we should be as permissive
        // as possible here and let the ConnectionManagers themselves throw
        // out the connect string if it doesn't make sense to them.
        LOG.warn("Could not determine scheme component of connect string");

        // Use the whole string.
        schemeStopIdx = connectStr.length();
      }
    }

    String scheme = connectStr.substring(0, schemeStopIdx);

    if (null == scheme) {
      // We don't know if this is a mysql://, hsql://, etc.
      // Can't do anything with this.
      LOG.warn("Null scheme associated with connect string.");
      return null;
    }

    LOG.debug("Trying with scheme: " + scheme);

    if (scheme.equals("http:")) {
      return new CouchbaseManager(options);
    } else {
      return null;
    }
  }
}
