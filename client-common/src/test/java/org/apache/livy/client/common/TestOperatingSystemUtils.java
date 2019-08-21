package org.apache.livy.client.common;

import com.sun.javafx.PlatformUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;

import static java.nio.file.attribute.PosixFilePermission.*;
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
        assertEquals(PlatformUtil.isUnix() || PlatformUtil.isMac(), OperatingSystemUtils.isPosixCompliant());
    }

    @Test
    public void testIsWindows() {
        assertEquals(PlatformUtil.isWindows(), OperatingSystemUtils.isWindows());
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
            value = OperatingSystemUtils.getBasedOnOs(
                    () -> "pass",
                    () -> "fail",
                    ""
            );
        } else if (OperatingSystemUtils.isWindows()) {
            value = OperatingSystemUtils.getBasedOnOs(
                    () -> "fail",
                    () -> "pass",
                    ""
            );
        }
        assertEquals(value, "pass");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetBasedOnOsThrowsException() {
        if (OperatingSystemUtils.isPosixCompliant()) {
           OperatingSystemUtils.getBasedOnOsThrowsException(
                    () -> {throw new IllegalArgumentException();},
                    () -> "fail",
                    ""
            );
        } else if (OperatingSystemUtils.isWindows()) {
            OperatingSystemUtils.getBasedOnOsThrowsException(
                    () -> "fail",
                    () -> {throw new IllegalArgumentException();},
                    ""
            );
        }
       fail();
    }

    @Test
    public void testSetOSAgnosticFilePermissions() throws IOException {
        File f = File.createTempFile("testFile", ".txt");
        f.deleteOnExit();
        OperatingSystemUtils.setOSAgnosticFilePermissions(f, EnumSet.of(OWNER_READ, OWNER_WRITE, OWNER_EXECUTE));
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