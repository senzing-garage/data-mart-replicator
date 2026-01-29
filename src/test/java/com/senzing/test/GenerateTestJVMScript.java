package com.senzing.test;

import java.io.*;

import static com.senzing.util.OperatingSystemFamily.*;

public class GenerateTestJVMScript {
    public static void main(String[] args) {
        String homePath = System.getProperty("user.home");
        File   homeDir  = (homePath == null) ? null : new File(homePath);
        String targetFileName = "target/java-wrapper/bin/java-wrapper.bat";
        if (args.length > 0) {
            targetFileName = args[0];
        }
        File targetFile = new File(targetFileName);
        File targetDir = targetFile.getParentFile();
        if (targetDir != null && !targetDir.exists()) {
            targetDir.mkdirs();
        }
        boolean devBuild = false;
        String devLibPath = System.getProperty("senzing.dev.lib.path");
        String senzingDirPath = System.getProperty("senzing.install.dir");
        String senzingPath = System.getProperty("senzing.path");
        if (senzingPath != null && senzingPath.trim().length() == 0) {
            senzingPath = null;
        }

        if ((senzingDirPath == null || senzingDirPath.trim().length() == 0)
                && senzingPath != null && senzingPath.trim().length() > 0) {
            File baseDir    = new File(senzingPath);
            File erDir      = new File(baseDir, "er");
            try {
                senzingDirPath = erDir.getCanonicalPath();

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        if (senzingDirPath != null && senzingDirPath.trim().length() == 0) {
            senzingDirPath = null;
        }

        String javaHome = System.getProperty("java.home");
        File javaHomeDir = new File(javaHome);
        File javaBinDir = new File(javaHomeDir, "bin");
        String javaExeName = (RUNTIME_OS_FAMILY == WINDOWS) ? "javaw.exe" : "java";
        File javaExecutable = new File(javaBinDir, javaExeName);
        File senzingDir = (senzingDirPath != null && senzingDirPath.trim().length() > 0)
                ? new File(senzingDirPath)
                : null;

        // check if we are building within the dev structure
        String[] directoryStructure = { "sz-sdk-java", "java", "g2", "apps", "dev" };
        File workingDir = new File(System.getProperty("user.dir"));
        File previousDir = null;
        boolean devStructure = true;
        for (String dirName : directoryStructure) {
            if (!workingDir.getName().equals(dirName)) {
                devStructure = false;
                break;
            }
            previousDir = workingDir;
            workingDir = workingDir.getParentFile();
        }

        // check if we are building in the product dev tree
        if (devStructure && senzingDir == null) {
            File buildDir = new File(previousDir, "build");
            if (buildDir.exists()) {
                senzingDir = new File(buildDir, "dist");
                try {
                    senzingDirPath = senzingDir.getCanonicalPath();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        // default the dev lib path to empty string if null
        if (devLibPath == null)
            devLibPath = "";

        // normalize the senzing directory path
        if (senzingDir != null) {
            String dirName = senzingDir.getName();
            if (senzingDir.exists() && senzingDir.isDirectory()) {
                if (dirName.equalsIgnoreCase("dist")) {
                    senzingDirPath = senzingDir.toString();
                    senzingDir = null;
                    devBuild = true;

                } else if (!dirName.equalsIgnoreCase("er")) {
                    if (RUNTIME_OS_FAMILY == MAC_OS) {
                        // for macOS be tolerant of Senzing.app or the electron app dir
                        if (dirName.equalsIgnoreCase("Senzing.app")) {
                            File contents = new File(senzingDir, "Contents");
                            File resources = new File(contents, "Resources");
                            senzingDir = new File(resources, "app");
                            dirName = senzingDir.getName();
                        }
                        if (dirName.equalsIgnoreCase("app")) {
                            senzingDir = new File(senzingDir, "er");
                        }

                    } else if (dirName.equalsIgnoreCase("senzing")) {
                        // for windows or linux allow the "Senzing" dir as well
                        senzingDir = new File(senzingDir, "er");
                    }
                    senzingDirPath = senzingDir.toString();
                    senzingDir = null;
                }
            }
        }

        System.out.println("*********************************************");
        System.out.println();
        System.out.println("senzing.path        = " + senzingPath);
        System.out.println("senzing.install.dir = " + senzingDirPath);
        System.out.println("java.home           = " + javaHome);
        System.out.println();
        System.out.println("*********************************************");

        File libDir = null;
        File platformLibDir = null;
        String pathSep = ":";
        String quote = "\"";
        if (senzingDirPath != null && senzingDirPath.trim().length() > 0) {
            senzingDir = new File(senzingDirPath);
        }
        switch (RUNTIME_OS_FAMILY) {
            case WINDOWS:
                if (senzingDir == null) {
                    File baseDir = new File(homeDir, "senzing");
                    senzingDir = new File(baseDir, "er");
                    if (!senzingDir.exists()) {
                        senzingDir = null;
                    }
                }
                if (senzingDir == null) {
                    senzingDir = new File("C:\\Program Files\\Senzing\\er");
                }
                libDir = new File(senzingDir, "lib");
                pathSep = ";";
                quote = "";
                break;
            case MAC_OS:
                if (senzingDir == null) {
                    File baseDir = new File(homeDir, "senzing");
                    senzingDir = new File(baseDir, "er");
                    if (!senzingDir.exists()) {
                        senzingDir = null;
                    }
                }
                if (senzingDir == null) {
                    senzingDir = new File("/Applications/Senzing.app/Contents/Resources/app/er");
                }
                libDir = new File(senzingDir, "lib");
                platformLibDir = new File(libDir, "macos");
                break;
            case UNIX:
                if (senzingDir == null) {
                    senzingDir = new File("/opt/senzing/er");
                }
                libDir = new File(senzingDir, "lib");
                platformLibDir = new File(libDir, "debian");
                break;
        }

        String libraryPath = quote + libDir.toString() + quote
                + ((platformLibDir != null)
                        ? pathSep + quote + platformLibDir + quote
                        : "");

        try {
            try (FileOutputStream fos = new FileOutputStream(targetFile);
                    OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
                    PrintWriter pw = new PrintWriter(osw)) {
                if (RUNTIME_OS_FAMILY == WINDOWS) {
                    if (devBuild) {
                        if (devLibPath.length() > 0)
                            devLibPath = ";" + devLibPath;
                    }
                    pw.println("@echo off");
                    pw.println("set Path=" + libraryPath + devLibPath + ";%Path%");
                    pw.println("\"" + javaExecutable.toString() + "\" %*");

                } else {
                    if (devBuild) {
                        if (devLibPath.length() > 0)
                            devLibPath = ":" + devLibPath;
                    }

                    pw.println("#!/bin/sh");
                    if (RUNTIME_OS_FAMILY == MAC_OS) {
                        pw.println("export DYLD_LIBRARY_PATH=" + libraryPath + devLibPath
                                + ":$DYLD_LIBRARY_PATH");
                    }
                    pw.println("export LD_LIBRARY_PATH=" + libraryPath + devLibPath
                            + ":$LD_LIBRARY_PATH");
                    pw.println("\"" + javaExecutable.toString() + "\" \"$@\"");
                }
            }
            targetFile.setExecutable(true);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
