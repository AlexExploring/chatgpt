package com.bytesforce.pub


import groovy.util.logging.Slf4j
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils
import org.springframework.core.io.support.PathMatchingResourcePatternResolver

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

@Slf4j
class MyFileUtils extends FileUtils {

    static boolean isFileLocked(String file) {
        return isFileLocked(new File(file))
    }

    static boolean isFileLocked(File file) {
        if (file == null) {
            return false
        }
        if (!file.exists()) {
            return false
        }
        return !file.renameTo(file)
    }

    static List<String> readLines(byte[] bytes, String encoding) {
        if (bytes == null || bytes.length == 0) {
            return null
        }
        if (MyStringUtils.isBlank(encoding)) {
            Panic.invalidParam("encoding should be identified when reading a file")
        }
        InputStream is = null
        BufferedReader bfReader = null
        List<String> list = []
        try {
            is = new ByteArrayInputStream(bytes)
            bfReader = new BufferedReader(new InputStreamReader(is, encoding))
            String line = null
            while ((line = bfReader.readLine()) != null) {
                list.add(line)
            }
            return list
        } finally {
            MyIOUtils.closeQuietly(is)
            MyIOUtils.closeQuietly(bfReader)
        }
    }

    static File createTmpFile(String fileNameWithSuffix) {
        String fileName = FilenameUtils.normalize(
                "${EnvUtils.tmpFolder}${File.separator}${fileNameWithSuffix}"
        )
        new File(fileName)
    }

    static byte[] readClasspathFile(String absFileName) {
        def res = new PathMatchingResourcePatternResolver().getResource(absFileName)
        if (!res.exists()) {
            return null
        }
        InputStream is = res.inputStream
        try {
            return is.bytes
        } finally {
            if (is != null) {
                MyIOUtils.closeQuietly(is)
            }
        }
    }

    static InputStream readClasspathOrFolderFileToSteam(String absFileName) {
        byte[] fdata = readClasspathOrFolderFileToBytes(absFileName)
        if (fdata == null) {
            return null
        } else {
            return new ByteArrayInputStream(fdata)
        }
    }

    static byte[] readClasspathOrFolderFileToBytes(String absFileName) {
        if (MyStringUtils.isBlank(absFileName)) {
            return null
        }
        if (absFileName.trim().startsWith("classpath")) {
            return readClasspathFile(absFileName)
        } else {
            if (absFileName.startsWith("file://")) {
                absFileName = absFileName - "file://"
            }
            return readFileToByteArray(new File(absFileName))
        }
//        def res = new PathMatchingResourcePatternResolver().getResource(absFileName)
//        if (res.exists()) {
//            return res.inputStream.bytes
//        } else {
//            def f = new File(absFileName)
//            if (f.exists()) {
//                return readFileToByteArray(f)
//            } else {
//                Panic.resourceNotFound("File not found: ${absFileName}")
//                return null
//            }
//        }
    }

    static boolean exists(String absFileName) {
        if (MyStringUtils.isBlank(absFileName)) {
            return false
        }
        sanitizeFilename(absFileName)
        if (absFileName.trim().startsWith("classpath")) {
            return new PathMatchingResourcePatternResolver().getResource(absFileName)?.exists()
        } else {
            if (absFileName.startsWith("file://")) {
                absFileName = absFileName - "file://"
            }
            return new File(absFileName).exists()
        }
    }

    static String readClasspathOrFolderFileToString(String absFileName) {
        byte[] fdata = readClasspathOrFolderFileToBytes(absFileName)
        if (fdata == null || fdata.length == 0) {
            return null
        }
        return new String(fdata, GlobalConst.UTF8)
    }

    static String sanitizeFilename(String inputName) {
        return inputName?.replaceAll("[^a-zA-Z0-9-_\\.]", "_")
    }

    static String guessContentType(String fileName) {
        URLConnection.guessContentTypeFromName(fileName)
    }


    static long displaySizeToByteCount(String displaySize) {
        if (MyStringUtils.isBlank(displaySize)) {
            Panic.invalidParam("displaySize should be provided")
        }
        displaySize = displaySize.trim().toUpperCase()
        if (MyNumberUtils.isNumber(displaySize)) {
            return MyNumberUtils.toNumber(displaySize).toLong()
        }
        if (displaySize.endsWith('KB')) {
            displaySize -= 'KB'
            if (MyNumberUtils.isNumber(displaySize)) {
                return MyNumberUtils.toNumber(displaySize).toLong() * ONE_KB
            } else {
                Panic.invalidParam("invalid display size [${displaySize}]")
            }
        } else if (displaySize.endsWith('MB')) {
            displaySize -= 'MB'
            if (MyNumberUtils.isNumber(displaySize)) {
                return MyNumberUtils.toNumber(displaySize).toLong() * ONE_MB
            } else {
                Panic.invalidParam("invalid display size [${displaySize}]")
            }
        } else if (displaySize.endsWith('GB')) {
            displaySize -= 'GB'
            if (MyNumberUtils.isNumber(displaySize)) {
                return MyNumberUtils.toNumber(displaySize).toLong() * ONE_GB
            } else {
                Panic.invalidParam("invalid display size [${displaySize}]")
            }
        } else if (displaySize.endsWith('TB')) {
            displaySize -= 'TB'
            if (MyNumberUtils.isNumber(displaySize)) {
                return MyNumberUtils.toNumber(displaySize).toLong() * ONE_TB
            } else {
                Panic.invalidParam("invalid display size [${displaySize}]")
            }
        } else if (displaySize.endsWith('PB')) {
            displaySize -= 'PB'
            if (MyNumberUtils.isNumber(displaySize)) {
                return MyNumberUtils.toNumber(displaySize).toLong() * ONE_PB
            } else {
                Panic.invalidParam("invalid display size [${displaySize}]")
            }
        } else if (displaySize.endsWith('EB')) {
            displaySize -= 'EB'
            if (MyNumberUtils.isNumber(displaySize)) {
                return MyNumberUtils.toNumber(displaySize).toLong() * ONE_EB
            } else {
                Panic.invalidParam("invalid display size [${displaySize}]")
            }
        } else {
            Panic.invalidParam("invalid display size [${displaySize}]")
        }
        return 0
    }

}

interface MyFileWriter {
    void write(List data)
}

class FileAppendBuilder implements MyFileWriter {
    private BufferedWriter bufferedWriter
    private String filePath

    private FileAppendBuilder() {
    }

    static FileAppendBuilder newBuilder() {
        return new FileAppendBuilder()
    }

    FileAppendBuilder init(String filepath, String content = null) {
        this.filePath = filePath
//        bufferedWriter = new BufferedWriter(new FileWriter(filepath, true))
        bufferedWriter = new BufferedWriter(new FileWriter(filepath, StandardCharsets.UTF_8, true))
        if (Objects.nonNull(content)) {
            bufferedWriter.write(content)
        }
        return this
    }

    FileAppendBuilder write(String content) {
        if (Objects.nonNull(content)) {
            bufferedWriter.write(content)
        }
        return this
    }

    void build() {
        if (Objects.isNull(bufferedWriter)) {
            Panic.logic("not init")
        }
        bufferedWriter.close()
    }

    static String toStr(Object obj) {
        try {
            if (obj instanceof Number) {
                return MyNumberUtils.format(obj, "#,##0.00")
            }
            if (Objects.isNull(obj)) {
                return ""
            }
            if (obj == 0) {
                return MyNumberUtils.format(BigDecimal.ZERO, "#,##0.00")
            }
            return obj.toString()
        } catch (Throwable t) {
            return obj?.toString()
        }

    }
    @Override
    void write(List data) {
        def line = data?.collect { MyStringUtils.csvHandlerStr(toStr(it)) }?.join(",")
        this.write(line ? line + System.lineSeparator() : "")
    }
}
