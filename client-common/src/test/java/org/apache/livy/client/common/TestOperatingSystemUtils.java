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

package org.apache.livy.client.common;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import static java.nio.file.attribute.PosixFilePermission.*;

import com.sun.javafx.PlatformUtil;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;


public class TestOperatingSystemUtils {

    @Before
    public void setUp() throws UnsupportedOperationException {
        if ( !(PlatformUtil.isMac() || PlatformUtil.isWindows() || PlatformUtil.isUnix())) {
            throw new UnsupportedOperationException("Not running on supported OS");
        }
    }

    @Test
    public void testIsPosixCompliant() {
        assertEquals(PlatformUtil.isUnix() || PlatformUtil.isMac(),
                     OperatingSystemUtils.isPosixCompliant());
    }

    @Test
    public void testIsWindows() {
        assertEquals(PlatformUtil.isWindows(),
                     OperatingSystemUtils.isWindows());
    }

    @Test
    public void testDoBasedOnOs() {
        if (OperatingSystemUtils.isPosixCompliant()) {
            OperatingSystemUtils.doBasedOnOs(
                    () -> {},
                    () -> fail(),
                    ""
            );
        } else if (OperatingSystemUtils.isWindows()) {
            OperatingSystemUtils.doBasedOnOs(
                    () -> fail(),
                    () -> {},
                    ""
            );
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDoBasedOnOsThrowsException() {
        if (OperatingSystemUtils.isPosixCompliant()) {
            OperatingSystemUtils.doBasedOnOsThrowsException(
                    () -> {throw new IllegalArgumentException();},
                    () -> fail(),
                    ""
            );
        } else if (OperatingSystemUtils.isWindows()) {
            OperatingSystemUtils.doBasedOnOsThrowsException(
                    () -> fail(),
                    () -> {throw new IllegalArgumentException();},
                    ""
            );
        }
    }

    @Test
    public void testGetBasedOnOs() {
        String value = "";
        if (OperatingSystemUtils.isPosixCompliant()) {
            value = OperatingSystemUtils.getBasedOnOS(
                    "pass",
                    "fail",
                    ""
            );
        } else if (OperatingSystemUtils.isWindows()) {
            value = OperatingSystemUtils.getBasedOnOS(
                    "fail",
                    "pass",
                    ""
            );
        }
        assertEquals(value, "pass");
    }

    @Test
    public void testSetOSAgnosticFilePermissions() throws IOException {
        File f = File.createTempFile("testFile", ".txt");
        f.deleteOnExit();
        OperatingSystemUtils.setOSAgnosticFilePermissions(f, EnumSet.of(OWNER_READ,
                                                                        OWNER_WRITE,
                                                                        OWNER_EXECUTE));
        assertTrue(f.canExecute());
        assertTrue(f.canWrite());
        assertTrue(f.canRead());

        OperatingSystemUtils.setOSAgnosticFilePermissions(f, EnumSet.of(OWNER_READ));
        if (OperatingSystemUtils.isPosixCompliant()) {
            assertFalse(f.canExecute());
        }
        assertFalse(f.canWrite());
        assertTrue(f.canRead());


    }
}