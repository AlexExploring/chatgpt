package com.bytesforce.insmate.local

import com.bytesforce.insmate.batchframework.JobParams
import com.bytesforce.insmate.batchframework.JobUtils
import com.bytesforce.insmate.bcp.gl.GLAttributes
import com.bytesforce.insmate.bcp.model.Bill
import com.bytesforce.insmate.bcp.model.CommissionSettle
import com.bytesforce.insmate.bcp.service.interf.BillService
import com.bytesforce.insmate.bcp.service.interf.CommissionService
import com.bytesforce.insmate.bcp.vo.BillESDoc
import com.bytesforce.insmate.pa.model.Endo
import com.bytesforce.insmate.pa.model.Policy
import com.bytesforce.insmate.pa.service.interf.EndoService
import com.bytesforce.insmate.pa.service.interf.PolicyService
import com.bytesforce.insmate.pa.vo.ScParticulars
import com.bytesforce.insmate.pub.BcpConst
import com.bytesforce.insmate.pub.BfBillQueryBuilder
import com.bytesforce.insmate.pub.PAConst
import com.bytesforce.insmate.pub.model.Job
import com.bytesforce.insmate.pub.model.ObjectStorage
import com.bytesforce.insmate.pub.model.ObjectStorageMetadata
import com.bytesforce.insmate.pub.service.interf.BlobService
import com.bytesforce.insmate.pub.service.interf.ScService
import com.bytesforce.insmate.pub.utils.BillFeeRelationshipUtils
import com.bytesforce.insmate.pub.utils.ESUtils
import com.bytesforce.insmate.report.model.PgReport
import com.bytesforce.insmate.report.model.Report
import com.bytesforce.insmate.report.service.interf.ReportService
import com.bytesforce.insmate.sys.model.Tenant
import com.bytesforce.insmate.sys.service.interf.TenantService
import com.bytesforce.pub.Bean
import com.bytesforce.pub.DateRange
import com.bytesforce.pub.GlobalConst
import com.bytesforce.pub.MyCollectionUtils
import com.bytesforce.pub.MyDateUtils
import com.bytesforce.pub.MyExcelUtils
import com.bytesforce.pub.MyIOUtils
import com.bytesforce.pub.MyJsonUtils
import com.bytesforce.pub.MyNumberUtils
import com.bytesforce.pub.MyStringUtils
import com.bytesforce.pub.ParamAssert
import com.bytesforce.pub.log.DslLog
import com.bytesforce.pub.vo.MyExcelParams
import org.elasticsearch.action.search.ClearScrollRequest
import org.elasticsearch.action.search.ClearScrollResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.search.SearchScrollRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.script.Script
import org.elasticsearch.script.ScriptType
import org.elasticsearch.search.Scroll
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.aggregations.bucket.terms.Terms

Job job = job
JobParams params = params

BlobService blobService = Bean.get(BlobService)
ReportService reportService = Bean.get(ReportService)
TenantService tenantService = Bean.get(TenantService)
ScService scService = Bean.get(ScService)
BillService billService = Bean.get(BillService)

class CommListSummaryItem {
    String scCode
    String scName
    String perfScCode
    String perfScName
    BigDecimal commission
    BigDecimal gstOnCommission
    BigDecimal commAndGst
    BigDecimal paidDuringCurtMonth
    BigDecimal notPaidDuringCurtMonth
}

class CommListDetailsItem {
    String scCode
    String scName
    String perfScCode
    String perfScName
    String transDate
    String effDate
    String inceptionDate
    String policyNo
    String productCode
    String refNo
    String policyHolderName
    String transType
    String fundType
    BigDecimal commission
    BigDecimal gstOnCommission
    BigDecimal commAndGst
    String settlementDate
}

List<String> currencyCodes = null
List<String> scCodes = null
DateRange dateRange = null
Date reportDate = null
Report report = null
CommListSummaryItem summaryItem = null
CommListDetailsItem detailsItem = null
List<CommListSummaryItem> listSummaryRows = new ArrayList<>()
List<CommListDetailsItem> listDetailsRows = new ArrayList<>()
ScParticulars sc = null
ScParticulars perfSc = null
boolean scChaned = false
OutputStream out = null
InputStream tpl = null

def getScript = {
    Script script = new Script(ScriptType.INLINE, "painless",
            "def commissionStatus = doc['commissionStatus.keyword'].value;\n" +
                    "if (commissionStatus == 'WAITING_FOR_ISSUE') {\n" +
                    "    return doc['dueDate'].value <= params.to;\n" +
                    "} else if (commissionStatus == 'PAID') {\n" +
                    "    return doc['fullySettledDate'].value <= params.to && doc['fullySettledDate'].value >= params.from;\n" +
                    "} else {\n" +
                    "    return true;\n" +
                    "}", ['from': dateRange.from.getTime(), 'to': dateRange.to.getTime()])
    return script
}

def getCurrencyCodesAndScCode = {
    SearchResponse currencyResp = BfBillQueryBuilder.build()
            .applyAuth()
            .billTypes([BcpConst.BILL_TYPE_DIRECT_COMMISSION, BcpConst.BILL_TYPE_OR_COMMISSION])
            .commissionStatuses([PAConst.COMMISSION_STATUS_PAID, PAConst.COMMISSION_STATUS_WAITING_FOR_ISSUE])
            .filterByScript(getScript())
            .diffFieldValue("currencyCode")
            .executeQuery()
    Terms currencyCodeAgg = currencyResp?.getAggregations()?.get("currencyCode")
    currencyCodes = currencyCodeAgg?.getBuckets()?.collect {
        it.getKeyAsString()
    }

    SearchResponse scResp = BfBillQueryBuilder.build()
            .applyAuth()
            .billTypes([BcpConst.BILL_TYPE_DIRECT_COMMISSION, BcpConst.BILL_TYPE_OR_COMMISSION])
            .commissionStatuses([PAConst.COMMISSION_STATUS_PAID, PAConst.COMMISSION_STATUS_WAITING_FOR_ISSUE])
            .filterByScript(getScript())
            .diffFieldValue("scCode")
            .executeQuery()
    Terms scCodeAgg = scResp?.getAggregations()?.get("scCode")
    scCodes = scCodeAgg?.getBuckets()?.collect {
        it.getKeyAsString()
    }
    scCodes = scCodes?.sort { a, b ->
        a <=> b
    }
}

//variables for processEsDoc
Policy policy = null
CommissionSettle commSettle = null
Endo endo = null

def processEsDoc = { String jsonEsDoc ->
    BillESDoc doc = MyJsonUtils.fromJson(jsonEsDoc, BillESDoc)
    Bill comm = billService.loadRawBill(doc.billId)
    Bill prem = BillFeeRelationshipUtils.getDirectPremBillByCommBill(comm.itntCode, comm.billId)
    GLAttributes glAttributes = GLAttributes.of(comm.jsonGLAttributes)
    policy = Bean.get(PolicyService).loadRawPolicy(comm.policyId)

    if (!scChaned) {
        BigDecimal commission = MyNumberUtils.nvl(comm.originalFeeAmount) * comm.drcr
        BigDecimal gstOnCommission = MyNumberUtils.nvl(comm.turnoverTaxAmount) * comm.drcr
        BigDecimal commAndGst = commission + gstOnCommission
        summaryItem.commission += commission
        summaryItem.gstOnCommission += gstOnCommission
        summaryItem.commAndGst += commAndGst
        summaryItem.paidDuringCurtMonth += comm.commissionStatus == PAConst.COMMISSION_STATUS_PAID ? (commAndGst * comm.drcr) : 0
        summaryItem.notPaidDuringCurtMonth += comm.commissionStatus != PAConst.COMMISSION_STATUS_PAID ? commAndGst : 0
    } else {
        scChaned = false

        //update sc info
        sc = scService.getScParticulars(job.itntCode, comm.scCode)
        perfSc = scService.getScParticulars(job.itntCode, sc.perfScCode)

        //new summary item
        summaryItem = new CommListSummaryItem()
        summaryItem.scCode = comm.scCode
        summaryItem.scName = sc.fullName()
        summaryItem.perfScCode = perfSc?.perfScCode
        summaryItem.perfScName = perfSc?.fullName()
        summaryItem.commission = MyNumberUtils.nvl(comm.originalFeeAmount) * comm.drcr
        summaryItem.gstOnCommission = MyNumberUtils.nvl(comm.turnoverTaxAmount) * comm.drcr
        summaryItem.commAndGst = summaryItem.commission + summaryItem.gstOnCommission
        summaryItem.paidDuringCurtMonth = comm.commissionStatus == PAConst.COMMISSION_STATUS_PAID ? (summaryItem.commAndGst * comm.drcr) : 0
        summaryItem.notPaidDuringCurtMonth = comm.commissionStatus != PAConst.COMMISSION_STATUS_PAID ? summaryItem.commAndGst : 0
        listSummaryRows.add(summaryItem)
    }

    detailsItem = new CommListDetailsItem()
    detailsItem.scCode = comm.scCode
    detailsItem.scName = sc.fullName()
    detailsItem.perfScCode = perfSc?.perfScCode
    detailsItem.perfScName = perfSc?.fullName()
    detailsItem.transDate = MyDateUtils.ddMMMyyyyStr(comm.billDate)
    if (MyStringUtils.isNotBlank(comm.commissionSettleId)) {
        commSettle = Bean.get(CommissionService).load(CommissionSettle, comm.commissionSettleId)
    }
    detailsItem.effDate = MyDateUtils.ddMMMyyyyStr(policy.effDate)
    String transType = null
    if (MyStringUtils.isNotBlank(comm.endoId)) {
        endo = Bean.get(EndoService).loadRawEndo(comm.endoId)
        detailsItem.effDate = MyDateUtils.ddMMMyyyyStr(endo.endoEffDate)
        if (endo.endoType == GlobalConst.ENDO_TYPE_INCEPTION_CANCELLATION ||
                endo.endoType == GlobalConst.ENDO_TYPE_MIDWAY_CANCELLATION) {
            transType = "Cancellation"
        } else {
            transType = "Endorsement"
        }
    } else {
        transType = policy.bizType == GlobalConst.BIZ_TYPE_RN ? "Renewal" : "New Business"
    }
    detailsItem.inceptionDate = MyDateUtils.ddMMMyyyyStr(comm.dueDate)
    detailsItem.policyNo = policy.policyNo
    detailsItem.productCode = comm.productCode
    detailsItem.refNo = prem == null ? null : prem.billRefNo
    detailsItem.policyHolderName = policy.policyholder?.fullName()
    detailsItem.transType = transType
    detailsItem.fundType = glAttributes.fundType
    detailsItem.commission = MyNumberUtils.nvl(comm.originalFeeAmount) * comm.drcr
    detailsItem.gstOnCommission = MyNumberUtils.nvl(comm.turnoverTaxAmount) * comm.drcr
    detailsItem.commAndGst = detailsItem.commission + detailsItem.gstOnCommission
    detailsItem.settlementDate = MyDateUtils.ddMMMyyyyStr(commSettle?.settleDate)
    commSettle = null
    listDetailsRows.add(detailsItem)
}

def processSearchHits = { SearchHit[] hits ->
    hits?.each {
        processEsDoc(it.sourceAsString)
    }
}

//variables for addExcelParams
List<MyExcelParams> excelParams = []

def addSummaryExcelParams = {String currencyCode ->
    if (MyCollectionUtils.isNotNullOrEmpty(listSummaryRows)) {
        excelParams.add(new MyExcelParams(
                name: "Summary " + currencyCode,
                rows: listSummaryRows,
                map: [
                        "from" : MyDateUtils.ddMMMyyyyStr(dateRange.from),
                        "to" : MyDateUtils.ddMMMyyyyStr(dateRange.to),
                        "generateDate" : MyDateUtils.ddMMMyyyyStr(MyDateUtils.now()),
                        "generateTime" : MyDateUtils.format(MyDateUtils.now(),MyDateUtils.HHmmss),
                ]
        ))
        listSummaryRows = new ArrayList<>()
    }
}

def addDetailsExcelParams = {String currencyCode ->
    if (MyCollectionUtils.isNotNullOrEmpty(listDetailsRows)) {
        excelParams.add(new MyExcelParams(
                name: "Details List " + currencyCode,
                rows: listDetailsRows,
                map: [
                        "from" : MyDateUtils.ddMMMyyyyStr(dateRange.from),
                        "to" : MyDateUtils.ddMMMyyyyStr(dateRange.to),
                        "generateDate" : MyDateUtils.ddMMMyyyyStr(MyDateUtils.now()),
                        "generateTime" : MyDateUtils.format(MyDateUtils.now(),MyDateUtils.HHmmss),
                ]
        ))
        listDetailsRows = new ArrayList<>()
    }
}

def createReportFile = {
    tpl = new ByteArrayInputStream(blobService.loadByFid(report.reportTemplateFid).fdata)
    out = new ByteArrayOutputStream()
    MyExcelUtils.createMoreSheetExcelWithTemplate(out, tpl, excelParams)

    String fileName = "Comm List_${MyDateUtils.format(reportDate, MyDateUtils.MMYYYY_FORMAT)}.xlsx"
    def os = blobService.createFile(new ObjectStorage(
            metadata: new ObjectStorageMetadata(
                    originalFileName: fileName,
                    mimeType: GlobalConst.MIME_TYPE_EXCEL_2007,
            ),
            fdata: out.toByteArray()
    ))

    Tenant tenant = tenantService.loadTenantByCode(job.itntCode, false)
    reportService.mergePgReport(new PgReport(
            itntCode: tenant.tenantCode,
            itntStructId: tenant.headquarter.structId,
            itntStructPath: tenant.headquarter.path,
            name: fileName,
            bizKey: fileName,
            reportDate: reportDate,
            reportType: report.reportCode,
            frequency: report.schedulePattern,
            fid: os.fid,
    ))

    excelParams = []
    if (tpl != null) {
        MyIOUtils.closeQuietly(tpl)
    }
    if (out != null) {
        MyIOUtils.closeQuietly(out)
    }
}

//variables for createReport
SearchResponse resp = null
int scrollSize = 100
String scrollId = null
Scroll scroll = new Scroll(TimeValue.timeValueMinutes(2L))
SearchHit[] searchHits = null

def createReport = {
    report = reportService.getReportByJobCode(job.itntCode, job.jobCode)
    ParamAssert.notEmpty(report.reportTemplateFid,
            "The Template of Report [${report.reportNameEn}] has not been defined, " +
                    "please complete report configuration")
    ParamAssert.notNull(report, "No report is linked the job [${job.jobCode}]")

    dateRange = DateRange.of(
            MyDateUtils.truncateTime(MyDateUtils.getFirstDayOfMonth(reportDate)),
            MyDateUtils.truncateTime(reportDate)
    )

    getCurrencyCodesAndScCode()
    DslLog.warn("alex: currencyCodes = ${scCodes?.join(",")}")
    for (int i = 0; i < currencyCodes?.size(); i++) {
        for(int j = 0; j < scCodes?.size(); j++) {
            scChaned = true

            resp = BfBillQueryBuilder.build()
                    .applyAuth()
                    .billTypes([BcpConst.BILL_TYPE_DIRECT_COMMISSION, BcpConst.BILL_TYPE_OR_COMMISSION])
                    .commissionStatuses([PAConst.COMMISSION_STATUS_PAID, PAConst.COMMISSION_STATUS_WAITING_FOR_ISSUE])
                    .filterByScript(getScript())
                    .currencyCode(currencyCodes.get(i))
                    .scCode(scCodes.get(j))
                    .orderByScCodeAsc()
                    .orderByCommStatus([PAConst.COMMISSION_STATUS_WAITING_FOR_ISSUE, PAConst.COMMISSION_STATUS_PAID])
                    .orderByCreatedAtAsc()
                    .size(scrollSize)
                    .executeScrollQuery(scroll)

            scrollId = resp.scrollId
            searchHits = resp.getHits().getHits()
            int pageIndex = 0
            while (searchHits != null && searchHits.length > 0) {
                if (DslLog.debugEnabled) {
                    DslLog.debug("${job.jobNameEn} processing pageIndex: ${pageIndex}")
                }
                processSearchHits(searchHits)

                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId)
                scrollRequest.scroll(scroll)
                resp = ESUtils.client().scroll(scrollRequest, RequestOptions.DEFAULT)
                searchHits = resp.getHits().getHits()
                pageIndex++
            }

            ClearScrollRequest clearScrollRequest = new ClearScrollRequest()
            clearScrollRequest.addScrollId(scrollId)
            ClearScrollResponse clearScrollResponse = ESUtils.client().clearScroll(clearScrollRequest, RequestOptions.DEFAULT)
            boolean succeeded = clearScrollResponse.isSucceeded()
            if (!succeeded) {
                DslLog.warn("the ES scroll of ${job.jobNameEn} has NOT been cleared correctly")
            }
        }

        addSummaryExcelParams(currencyCodes.get(i))
        addDetailsExcelParams(currencyCodes.get(i))
    }
    createReportFile()
}

try {
    //start execute job
    reportDate = MyDateUtils.truncateTime(JobUtils.getJobProcessDate(params, MyDateUtils.yesterday()))
    createReport(reportDate)
    JobUtils.jobExecStats = "Report is created"

} finally {
    if (tpl != null) {
        MyIOUtils.closeQuietly(tpl)
    }
    if (out != null) {
        MyIOUtils.closeQuietly(out)
    }
}
