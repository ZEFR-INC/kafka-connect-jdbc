/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.util;

import java.util.HashMap;
import java.util.Map;

/**
 * General string utilities that are missing from the standard library and may commonly be
 * required by Connector or Task implementations.
 */
public class StringUtils {

  /**
   * Generate a String by appending all the @{elements}, converted to Strings, delimited by
   * @{delim}.
   * @param elements list of elements to concatenate
   * @param delim delimiter to place between each element
   * @return the concatenated string with delimiters
   */
  public static <T> String join(Iterable<T> elements, String delim) {
    StringBuilder result = new StringBuilder();
    boolean first = true;
    for (T elem : elements) {
      if (first) {
        first = false;
      } else {
        result.append(delim);
      }
      result.append(elem);
    }
    return result.toString();
  }

  /**
   * Generate a Map from a String where the string is a list of key value pairs.
   * @param input a String of key value pairs
   * @param delim delimiter placed in between each key value pair
   * @param keyValueSeparator the separator between the key and value in a pair
   * @return a Map containing the elements represented in the input String
   */
  public static Map<String, String> stringToMap(
      String input, String delim, String keyValueSeparator
  ) throws IllegalArgumentException {
    Map<String, String> result = new HashMap<>();
    String[] keyValuePairs = input.split(delim);
    for (String pair : keyValuePairs) {
      String[] keyValue = pair.split(keyValueSeparator);
      if (keyValue.length == 2) {
        result.put(keyValue[0], keyValue[1]);
      } else {
        throw new IllegalArgumentException(String.format(
          "Could not parse key value pair '%s' with separator '%s'.", pair, keyValueSeparator));
      }
    }
    return result;
  }
}
