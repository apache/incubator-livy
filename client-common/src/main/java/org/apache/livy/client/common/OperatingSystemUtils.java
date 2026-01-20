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
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collections;
import java.util.EnumSet;
import static java.nio.file.attribute.PosixFilePermission.*;

import com.sun.javafx.PlatformUtil;

/*
 * Utility class to get OS related information and perform OS-dependent actions
 */
public class OperatingSystemUtils {

    public static boolean isPosixCompliant() {
        return PlatformUtil.isMac() || PlatformUtil.isUnix();
    }

    public static boolean isWindows() {
        return PlatformUtil.isWindows();
    }

    public static void doBasedOnOs(
            Procedure doWhenPosixCompliant,
            Procedure doWhenWindows,
            String operationDescription) {
        doBasedOnOsThrowsException(
                () -> doWhenPosixCompliant.execute(),
                () -> doWhenWindows.execute(),
                operationDescription);
    }

    public static <ExceptionType extends Exception> void doBasedOnOsThrowsException(
            ThrowingProcedure<ExceptionType> doWhenPosixCompliant,
            ThrowingProcedure<ExceptionType> doWhenWindows,
            String operationDescription) throws ExceptionType {
        getBasedOnOS(doWhenPosixCompliant, doWhenWindows, operationDescription).execute();
    }

    public static <T> T getBasedOnOS(
            T getWhenPosixCompliant,
            T getWhenWindows,
            String operationDescription) {
        if (isPosixCompliant()) {
            return getWhenPosixCompliant;
        } else if (isWindows()) {
            return getWhenWindows;
        } else {
            String seperator = operationDescription.isEmpty() ? "" : ": ";
            throw new UnsupportedOperationException("Operation" + seperator + operationDescription +
                                                            " is not supported on this OS");
        }
    }

    public static void setOSAgnosticFilePermissions(File file,
                                                    EnumSet<PosixFilePermission> permissions)
            throws IOException {
        OperatingSystemUtils.doBasedOnOsThrowsException(
                () -> Files.setPosixFilePermissions(file.toPath(), permissions),
                //whether or not a file is read-only is the only applicable permission on Windows
                () -> file.setWritable(isWriteable(permissions), permissions.contains(OWNER_WRITE)),
                "Setting file permissions"
                                                       );
    }

    private static boolean isWriteable(EnumSet<PosixFilePermission> permissions) {
        return !Collections.disjoint(permissions, EnumSet.of(OWNER_WRITE,
                                                             GROUP_WRITE,
                                                             OTHERS_WRITE));
    }

    @FunctionalInterface
    public interface Procedure {
        void execute();
    }

    @FunctionalInterface
    public interface ThrowingProcedure<ExceptionType extends Exception> {
        void execute() throws ExceptionType;
    }


}
