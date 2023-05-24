package com.bytesforce.pub

import com.alibaba.excel.EasyExcel
import com.alibaba.excel.EasyExcelFactory
import com.alibaba.excel.write.metadata.WriteSheet
import com.alibaba.excel.write.metadata.fill.FillConfig
import com.alibaba.excel.write.metadata.fill.FillWrapper
import com.bytesforce.pub.vo.MyExcelParams
import groovy.util.logging.Slf4j
import org.apache.poi.hssf.usermodel.HSSFDateUtil
import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.CellType
import org.apache.poi.ss.util.NumberToTextConverter

import java.text.DecimalFormat

@Slf4j
final class MyExcelUtils {
    private MyExcelUtils() {}
    private static DecimalFormat decimalFormat = new DecimalFormat("0")

    static final boolean isXSSF(String fileName) {
        fileName?.toLowerCase()?.trim()?.endsWith(".xlsx")
    }

    static final String getCellVal(Cell cell) {
        String str
        switch (cell.getCellType()) {
            case CellType.BLANK:
                str = ""
                break
            case CellType.BOOLEAN:
                str = String.valueOf(cell.getBooleanCellValue())
                break
            case CellType.FORMULA:
                // 如果是 excel 公式，计算后取四位小数，预防 double 计算精度问题
                str = MyNumberUtils.round(cell.getNumericCellValue(), 4).toString()
                break
            case CellType.NUMERIC:
                if (HSSFDateUtil.isCellDateFormatted(cell)) {
                    str = MyDateUtils.format(cell.getDateCellValue())
                } else {
                    str = NumberToTextConverter.toText(cell.getNumericCellValue())
                    if (str && str.matches("\\d{0,}.0{1,}")) {
                        str = decimalFormat.format(Double.valueOf(str))
                    }
                }
                break
            case CellType.STRING:
                str = cell.getStringCellValue()
                break
            default:
                str = null
                break
        }
        return str
    }

    /**
     * download excel by template
     * @param out Data output stream
     * @param tpl Template input stream
     * @param data list data
     * @param map other data
     */
    static void createExcelWithTemplate(OutputStream out, InputStream tpl, Object data, Map map = null, String sheetName = null) {
        if (!out) {
            Panic.invalidParam("outputStream can not be null")
        }
        if (!tpl) {
            Panic.invalidParam("inputStream can not be null")
        }
        if (!data) {
            Panic.invalidParam("data can not be null")
        }
        def excelWriter = EasyExcelFactory.write(out).withTemplate(tpl).build()
        WriteSheet writeSheet = EasyExcelFactory.writerSheet().build()
        if (MyStringUtils.isNotBlank(sheetName)) {
            writeSheet.sheetName = sheetName
        }
        excelWriter.fill(data, writeSheet)
        if (!MyCollectionUtils.isNullOrEmptyMap(map)) {
            excelWriter.fill(map, writeSheet)
        }
        excelWriter.finish()
    }

    /**
     * download excel by template
     */
    static void createMoreSheetExcelWithTemplate(OutputStream out, InputStream tpl, Map... maps) {
        try {
            log.info("member listing exec excel")
            if (!out) {
                Panic.invalidParam("outputStream can not be null")
            }
            if (!tpl) {
                Panic.invalidParam("inputStream can not be null")
            }
            int sheetNo = 0
            def excelWriter = EasyExcelFactory.write(out).withTemplate(tpl).build()
            maps.each {
                log.info("member listing map data: ${MyJsonUtils.toJson(it)}")
                Object data = it.get("data")
                Map map = (Map) it.get("map")
                if (!data) {
                    sheetNo++
                    return
                }
                WriteSheet writeSheet = EasyExcelFactory.writerSheet(sheetNo).build()
                excelWriter.fill(data, writeSheet)
                if (!MyCollectionUtils.isNullOrEmptyMap(map)) {
                    excelWriter.fill(map, writeSheet)
                }
                sheetNo++
            }
            excelWriter.finish()
        } catch (Throwable ex) {
            log.error("create Excel with template failed", ex)
        }
    }

    /**
     * download excel by template
     */
    static void createMoreSheetExcelWithTemplates(OutputStream out, InputStream tpl, Map... maps) {
        try {
            if (!out) {
                Panic.invalidParam("outputStream can not be null")
            }
            if (!tpl) {
                Panic.invalidParam("inputStream can not be null")
            }
            int sheetNo = 0
            def excelWriter = EasyExcelFactory.write(out).withTemplate(tpl).build()
            maps.each {
                Object data = it.get("data")
                Map map = (Map) it.get("map")
//                if (!data) {
//                    Panic.invalidParam("data can not be null")
//                }
                WriteSheet writeSheet = EasyExcelFactory.writerSheet(sheetNo).build()
                excelWriter.fill(data, writeSheet)
                if (!MyCollectionUtils.isNullOrEmptyMap(map)) {
                    excelWriter.fill(map, writeSheet)
                }
                sheetNo++
            }
            excelWriter.finish()
        } catch (Throwable ex) {
            log.error("create Excel with template failed", ex)
        }
    }

    /**
     * Fill an Excel template file with multiple sheets
     * @param out
     * @param tpl
     * @param params
     */
    static void createMoreSheetExcelWithTemplate(OutputStream out, InputStream tpl, List<MyExcelParams> params) {
        if (!out) {
            Panic.invalidParam("outputStream can not be null")
        }
        if (!tpl) {
            Panic.invalidParam("inputStream can not be null")
        }
        if (MyCollectionUtils.isNullOrEmpty(params)) {
            Panic.invalidParam("data can not be null")
        }
        try {
            def excelWriter = EasyExcelFactory.write(out).withTemplate(tpl).build()
            params.eachWithIndex { MyExcelParams param, int i ->
                if (param.rows != null || !MyCollectionUtils.isNullOrEmptyMap(param.map)) {
                    WriteSheet writeSheet = EasyExcelFactory.writerSheet(i).build()
                    if (MyStringUtils.isNotBlank(param.name)) {
                        writeSheet.sheetName = param.name
                    }
                    if (param.rows != null) {
                        excelWriter.fill(param.rows, writeSheet)
                    }
                    if (!MyCollectionUtils.isNullOrEmptyMap(param.map)) {
                        excelWriter.fill(param.map, writeSheet)
                    }
                }
            }
            excelWriter.finish()
        } catch (Throwable ex) {
            log.error("create Excel with template failed", ex)
        }
    }

    /**
     * 文档参照 {@see https://easyexcel.opensource.alibaba.com/docs/3.0.x/quickstart/fill#%E5%AF%B9%E8%B1%A1-1}
     * 同sheet多表格写入</br>
     * excelWriter.fill(new FillWrapper("data1", data()), fillConfig, writeSheet);
     * excelWriter.fill(new FillWrapper("data2", data()), fillConfig, writeSheet);
     */
    static void complexFill(OutputStream outTarget, InputStream templateStream, Integer sheetNo, Map<String, List<Object>> tableData, Map<String, Object> otherParam = null) {
        com.alibaba.excel.ExcelWriter excelWriter = EasyExcel.write(outTarget).withTemplate(templateStream).build()
        WriteSheet writeSheet = EasyExcel.writerSheet(sheetNo).build()
        FillConfig fillConfig = FillConfig.builder().forceNewRow(Boolean.TRUE).build()

        //填充表数据
        if (MyMapUtils.isNotEmpty(tableData)) {
            tableData.each {
                log.info("fill table: ${it.key}, size: ${it.value.size()}")
                excelWriter.fill(new FillWrapper(it.key, it.value), fillConfig, writeSheet)
            }
        }
        //补充其余参数
        if (otherParam == null) {
            otherParam = new HashMap<String, Object>()
        }
        otherParam.put("now", new Date())
        excelWriter.fill(otherParam, writeSheet)
        excelWriter.finish()
    }
}


