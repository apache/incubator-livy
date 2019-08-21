package org.apache.livy.client.common;

import com.sun.javafx.PlatformUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collections;
import java.util.EnumSet;
import java.util.function.Supplier;

import static java.nio.file.attribute.PosixFilePermission.*;

public class OperatingSystemUtils {


    @FunctionalInterface
    public interface Procedure {
        void execute();
    }

    @FunctionalInterface
    public interface ThrowingProcedure<ExceptionType extends Exception> {
        void execute() throws ExceptionType;
    }

    @FunctionalInterface
    public interface ThrowingSupplier<ExceptionType extends Exception, T> {
        T get() throws ExceptionType;
    }

    public static boolean isPosixCompliant() {
        return PlatformUtil.isMac() || PlatformUtil.isUnix();
    }

    public static boolean isWindows() {
        return PlatformUtil.isWindows();
    }

    public static void doBasedOnOs( Procedure doWhenPosixCompliant,  Procedure doWhenWindows, String operationDescription) {
        doBasedOnOsThrowsException(() -> doWhenPosixCompliant.execute(), () -> doWhenWindows.execute(), operationDescription);
    }

    public static <ExceptionType extends Exception> void doBasedOnOsThrowsException( ThrowingProcedure<ExceptionType> doWhenPosixCompliant,  ThrowingProcedure<ExceptionType> doWhenWindows, String operationDescription) throws ExceptionType {
        getBasedOnOsThrowsException(
                () -> {doWhenPosixCompliant.execute(); return null;},
                () -> {doWhenWindows.execute(); return null;},
                operationDescription);
    }

    public static <T> T getBasedOnOs( Supplier<T> doWhenPosixCompliant, Supplier<T> doWhenWindows, String operationDescription) {
        return getBasedOnOsThrowsException(() -> doWhenPosixCompliant.get(), () -> doWhenWindows.get(), operationDescription);
    }

    public static <ExceptionType extends Exception, T> T getBasedOnOsThrowsException(ThrowingSupplier<ExceptionType, T> doWhenPosixCompliant, ThrowingSupplier<ExceptionType, T> doWhenWindows, String operationDescription) throws ExceptionType {
        if (isPosixCompliant()) {
            return doWhenPosixCompliant.get();
        } else if ( isWindows()) {
            return doWhenWindows.get();
        } else {
            String seperator = operationDescription.isEmpty() ? "" : ": ";
            throw new UnsupportedOperationException("Operation" + seperator + operationDescription + " is not supported on this OS");
        }
    }

    public static void setOSAgnosticFilePermissions(File file, EnumSet<PosixFilePermission> permissions) throws IOException {
        OperatingSystemUtils.doBasedOnOsThrowsException(
                () -> Files.setPosixFilePermissions(file.toPath(), permissions),
                () -> file.setWritable(isWriteable(permissions), permissions.contains(OWNER_WRITE)), //whether or not a file is read-only is the applicable permission on Windows
                "Setting file permissions"
        );
    }

    private static boolean isWriteable(EnumSet<PosixFilePermission> permissions) {
        return !Collections.disjoint(permissions, EnumSet.of(OWNER_WRITE, GROUP_WRITE, OTHERS_WRITE));
    }



}
