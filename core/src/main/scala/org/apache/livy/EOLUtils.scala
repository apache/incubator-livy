/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.livy

/**
 * Helper class to deal with end-of-line markers in text files.
 */
object EOLUtils {
  /** Unix-style end-of-line marker (LF) */
  private val EOL_UNIX: String = "\n"

  /** Windows-style end-of-line marker (CRLF) */
  private val EOL_WINDOWS: String = "\r\n"

  /** "Old Mac"-style end-of-line marker (CR) */
  private val EOL_OLD_MAC: String = "\r"

  /** Default end-of-line marker on current syste */
  private val EOL_SYSTEM_DEFAULT: String = System.getProperty("line.separator")

  object Mode extends Enumeration {
    type Mode = Value

    val LF, CRLF, CR = Value

    lazy val SYSTEM_DEFAULT: Mode = {
      val tmp = if (EOL_SYSTEM_DEFAULT == EOL_UNIX) {
        LF
      } else if (EOL_SYSTEM_DEFAULT == EOL_WINDOWS) {
        CRLF
      } else if (EOL_SYSTEM_DEFAULT == EOL_OLD_MAC) {
        CR
      } else {
        null
      }

      if (tmp == null) {
        throw new IllegalStateException("Could not determine system default end-of-line marker")
      }
      tmp
    }

    private def determineEOL(s: String): Mode = {
      val charArray = s.toCharArray

      var prev: Char = null.asInstanceOf[Char]
      for (ch <- charArray) {
        if (ch == '\n') {
          if (prev == '\r') {
            return CRLF
          } else {
            return LF
          }
        } else if (prev == '\r') {
          return CR
        }

        prev = ch
      }

      null
    }

    def hasWindowsEOL(s: String): Boolean = determineEOL(s) == CRLF

    def hasUnixEOL(s: String): Boolean = determineEOL(s) == LF

    def hasOldMacEOL(s: String): Boolean = determineEOL(s) == CR

    def hasSystemDefaultEOL(s: String): Boolean = determineEOL(s) == SYSTEM_DEFAULT
  }

  def convertToSystemEOL(s: String): String = convertLineEndings(s, EOL_SYSTEM_DEFAULT)

  private def convertLineEndings(s: String, eol: String): String = {
    if (Mode.hasWindowsEOL(s)) {
      s.replaceAll(EOL_WINDOWS, eol)
    } else if (Mode.hasUnixEOL(s)) {
      s.replaceAll(EOL_UNIX, eol)
    } else if (Mode.hasOldMacEOL(s)) {
      s.replaceAll(EOL_OLD_MAC, eol)
    } else {
      s
    }
  }
}
