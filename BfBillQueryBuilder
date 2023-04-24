package com.bytesforce.insmate.pub


import com.bytesforce.insmate.pa.service.interf.MaService
import com.bytesforce.insmate.pa.vo.MaQueryParams
import com.bytesforce.insmate.pub.service.interf.TransService
import com.bytesforce.insmate.pub.utils.ESUtils
import com.bytesforce.insmate.pub.utils.TimeZoneUtils
import com.bytesforce.insmate.pub.utils.UserUtils
import com.bytesforce.insmate.pub.vo.MonthRange
import com.bytesforce.insmate.pub.vo.PolicyQueryParams
import com.bytesforce.pub.*
import groovy.util.logging.Slf4j
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.script.Script
import org.elasticsearch.search.Scroll
import org.elasticsearch.search.aggregations.AggregationBuilder
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.BucketOrder
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval
import org.elasticsearch.search.aggregations.bucket.histogram.ExtendedBounds
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder

@Slf4j
class BfBillQueryBuilder extends BaseBfQueryBuilder {
    private SearchSourceBuilder ssb = new SearchSourceBuilder()
    private BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
    private AggregationBuilder rootAggs
    private AggregationBuilder parentAggs
    private Integer size
    private Integer from

    BfBillQueryBuilder() {}

    static BfBillQueryBuilder build() {
        new BfBillQueryBuilder()
    }

    BfBillQueryBuilder size(Integer size) {
        if (size > 200) {
            this.size = 200
        } else if (size <= 0) {
            this.size = 10
        } else {
            this.size = size
        }
        return this
    }

    BfBillQueryBuilder from(Integer from) {
        this.from = from
        return this
    }

    BfBillQueryBuilder groupBy(String aggsName, String fieldName) {
        def terms = AggregationBuilders.terms(aggsName).field(fieldName)
        if (parentAggs != null) {
            parentAggs.subAggregation(terms)
        }
        parentAggs = terms
        if (rootAggs == null) {
            rootAggs = parentAggs
        }
        return this
    }

    BfBillQueryBuilder groupByAndNCount(String aggsName, String fieldName, int size) {
        def terms = AggregationBuilders.terms(aggsName).field(fieldName).size(size)
        if (parentAggs != null) {
            parentAggs.subAggregation(terms)
        }
        parentAggs = terms
        if (rootAggs == null) {
            rootAggs = parentAggs
        }
        return this
    }

    BfBillQueryBuilder count(String aggsName) {
        def count = AggregationBuilders.count(aggsName).field("billId.keyword")
        if (parentAggs != null) {
            parentAggs.subAggregation(count)
        }
        parentAggs = count
        if (rootAggs == null) {
            rootAggs = parentAggs
        }
        return this
    }

    BfBillQueryBuilder sumAmount(String aggsName) {
        def sum = AggregationBuilders.sum(aggsName).field("amount")
        if (parentAggs != null) {
            parentAggs.subAggregation(sum)
        }
        parentAggs = sum
        if (rootAggs == null) {
            rootAggs = parentAggs
        }
        return this
    }

    BfBillQueryBuilder sumBalance(String aggsName) {
        def sum = AggregationBuilders.sum(aggsName).field("balance")
        if (parentAggs != null) {
            parentAggs.subAggregation(sum)
        }
        parentAggs = sum
        if (rootAggs == null) {
            rootAggs = parentAggs
        }
        return this
    }

    BfBillQueryBuilder sumBalanceWithDrcr(String aggsName) {
        def sum = AggregationBuilders.sum(aggsName).script(new Script("doc['drcr'].value * doc['balance'].value"))
        if (parentAggs != null) {
            parentAggs.subAggregation(sum)
        }
        parentAggs = sum
        if (rootAggs == null) {
            rootAggs = parentAggs
        }
        return this
    }

    /**
     * note: root and parent aggregation must exist, otherwise null pointer exception will be raised
     */
    BfBillQueryBuilder sumAmountAndBalance(String amountAggsName, String balanceAggsName) {
//        parentAggs.subAggregation(AggregationBuilders.sum(amountAggsName).field("amount"))
//        parentAggs.subAggregation(AggregationBuilders.sum(balanceAggsName).field("balance"))
        parentAggs.subAggregation(AggregationBuilders.sum(amountAggsName).script(new Script("doc['drcr'].value * doc['amount'].value")))
        parentAggs.subAggregation(AggregationBuilders.sum(balanceAggsName).script(new Script("doc['drcr'].value * doc['balance'].value")))
        this
    }

    BfBillQueryBuilder avg(String aggsName, String field) {
        def avg = AggregationBuilders.avg(aggsName).field(field)
        if (parentAggs != null) {
            parentAggs.subAggregation(avg)
        }
        parentAggs = avg
        if (rootAggs == null) {
            rootAggs = parentAggs
        }
        return this
    }

    /**
     * ranges:
     * each element must have the following keys:
     * 1. key: key of range
     * 2. from: value of range from, included. use elastic search syntax, can be null.
     * 3. to: value of range to, excluded. use elastic search syntax, can be null.
     */
    BfBillQueryBuilder byDateRanges(String aggregationName, String fieldName, List<Map<String, String>> ranges) {
        def histogram = AggregationBuilders.dateRange(aggregationName)
                .field(fieldName)
                .with { builder ->
                    ranges.each {
                        builder.addRange(it.key, it.from, it.to)
                    }
                    builder
                }
                .format("yyyy/MM/dd")
                .timeZone(TimeZoneUtils.timeZoneId)

        if (parentAggs != null) {
            parentAggs.subAggregation(histogram)
        }
        parentAggs = histogram
        if (rootAggs == null) {
            rootAggs = parentAggs
        }
        return this
    }

    BfBillQueryBuilder monthHistogram(String aggsName, MonthRange range) {
        if (range == null) {
            range = new MonthRange()
        }
        if (range.from == null) {
            range.from = MyDateUtils.now().format("yyyyMM")?.toInteger()
        }
        if (range.to == null) {
            range.to = MyDateUtils.now().format("yyyyMM")?.toInteger()
        }
        def dateHistogram = AggregationBuilders.dateHistogram(aggsName)
                .field("billDate")
                .minDocCount(0)
                .dateHistogramInterval(DateHistogramInterval.MONTH)
                .format("MM/yyyy")
                .timeZone(TimeZoneUtils.timeZoneId)
                .order(BucketOrder.key(true))
                .extendedBounds(new ExtendedBounds(range.formattedFrom("MM/yyyy"), range.formattedTo("MM/yyyy")))
        if (parentAggs != null) {
            parentAggs.subAggregation(dateHistogram)
        }
        parentAggs = dateHistogram
        if (rootAggs == null) {
            rootAggs = parentAggs
        }
        return this
    }

    BfBillQueryBuilder percentileRanks(String aggsName, BigDecimal value) {
        percentileRanks(aggsName, [value])
    }

    BfBillQueryBuilder percentileRanks(String aggsName, List<BigDecimal> values) {
        double[] doubleValues = (Double[]) values?.collect {
            it?.doubleValue()
        }?.toArray()
        def ranks = AggregationBuilders.percentileRanks(aggsName, doubleValues).field("amount")
        if (parentAggs != null) {
            parentAggs.subAggregation(ranks)
        }
        parentAggs = ranks
        if (rootAggs == null) {
            rootAggs = parentAggs
        }
        return this
    }

    BfBillQueryBuilder applyAuth() {
        if (UserUtils.isPlatformUserLoggedIn()) {
            //has authority
        } else if (UserUtils.isInsurerLoggedIn()) {
            def itntCode = AppContext.instance.loginUser.tenantCode
            boolQueryBuilder.must(QueryBuilders.termQuery("itntCode.keyword", itntCode))
        } else if (UserUtils.isChannelLoggedIn()) {
            def path = AppContext.instance.loginUser.structPath
            boolQueryBuilder.must(QueryBuilders.matchPhrasePrefixQuery("ctntStructPath", path))
        } else if (UserUtils.isDirectCustomerButNotAnonymousLoggedIn()) {
            MaService maService = Bean.get(MaService)
            List<String> maIds = maService.queryMas(new MaQueryParams(
                    pageIndex: 1,
                    pageSize: 50
            ))?.items?.collect { it.maId }
            TransService transService = Bean.get(TransService)
            List<String> policyIds = transService.queryPolicies(new PolicyQueryParams(
                    pageIndex: 1,
                    pageSize: 50
            ))?.items?.collect { it.refId }
            def loginUser = AppContext.instance.loginUser
            //当该用户未实名, 或者名下没有master policy, policy时不允许查出Bill
            if (loginUser.certified != GlobalConst.YES ||
                    (MyCollectionUtils.isNullOrEmpty(policyIds) && MyCollectionUtils.isNullOrEmpty(maIds))) {
                boolQueryBuilder.must(QueryBuilders.scriptQuery(new Script("1!=1")))
            } else {
                BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                if (MyCollectionUtils.isNotNullOrEmpty(policyIds)) {
                    queryBuilder.should(QueryBuilders.termsQuery("policyId.keyword", policyIds))
                }
                if (MyCollectionUtils.isNotNullOrEmpty(maIds)) {
                    queryBuilder.should(QueryBuilders.termsQuery("maId.keyword", maIds))
                }
                boolQueryBuilder.must(queryBuilder)
            }
        } else {
            Panic.noAuth("has no authority")
        }
        return this
    }

    BfBillQueryBuilder ids(List<String> billIds) {
        if (MyCollectionUtils.isNullOrEmpty(billIds)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termsQuery("billId.keyword", billIds))
        return this
    }

    BfBillQueryBuilder key(String key) {
        if (MyStringUtils.isBlank(key)) {
            return this
        }
        boolQueryBuilder.must(queryKeyBuilder(key))
        return this
    }

    BfBillQueryBuilder tags(List<String> tags) {
        if (MyCollectionUtils.isNullOrEmpty(tags)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termsQuery("tags.keyword", tags))
        return this
    }

    BfBillQueryBuilder productCates(List<String> productCates) {
        if (MyCollectionUtils.isNullOrEmpty(productCates)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termsQuery("productCate.keyword", productCates))
        return this
    }

    BfBillQueryBuilder productCodes(List<String> productCodes) {
        if (MyCollectionUtils.isNullOrEmpty(productCodes)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termsQuery("productCode.keyword", productCodes))
        return this
    }

    BfBillQueryBuilder commissionStatuses(List<String> commissionStatuses) {
        if (MyCollectionUtils.isNullOrEmpty(commissionStatuses)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termsQuery("commissionStatus.keyword", commissionStatuses))
        return this
    }

    BfBillQueryBuilder commissionSubStatuses(List<String> commissionSubStatuses) {
        if (MyCollectionUtils.isNullOrEmpty(commissionSubStatuses)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termsQuery("commissionSubStatus.keyword", commissionSubStatuses))
        return this
    }

    BfBillQueryBuilder commissionStatusesAndSubStatuses(List<String> commissionStatusesOnly, List<String> commissionStatusesBy, List<String> commissionSubStatuses) {
        if (MyCollectionUtils.isNullOrEmpty(commissionStatusesOnly)
                || MyCollectionUtils.isNullOrEmpty(commissionStatusesBy)
                || MyCollectionUtils.isNullOrEmpty(commissionSubStatuses)) {
            return this
        }
        boolQueryBuilder.must(
                QueryBuilders.boolQuery()
                        .should(QueryBuilders.termsQuery("commissionStatus.keyword", commissionStatusesOnly))
                        .should(
                                QueryBuilders.boolQuery()
                                        .must(QueryBuilders.termsQuery("commissionStatus.keyword", commissionStatusesBy))
                                        .mustNot(QueryBuilders.termsQuery("commissionSubStatus.keyword", commissionSubStatuses))
                        )
        )
        return this
    }

    BfBillQueryBuilder scCates(List<String> scCates) {
        if (MyCollectionUtils.isNullOrEmpty(scCates)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termsQuery("scCate.keyword", scCates))
        return this
    }

    BfBillQueryBuilder scCode(String scCode) {
        if (MyStringUtils.isBlank(scCode)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termQuery("scCode.keyword", scCode))
        return this
    }

    BfBillQueryBuilder perfScCode(String perfScCode) {
        if (MyStringUtils.isBlank(perfScCode)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termQuery("perfScCode.keyword", perfScCode))
        return this
    }

    BfBillQueryBuilder scCodes(List<String> scCodes) {
        if (MyCollectionUtils.isNullOrEmpty(scCodes)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termsQuery("scCode.keyword", scCodes))
        return this
    }

    BfBillQueryBuilder riStakeholderCode(String riStakeholderCode) {
        if (MyStringUtils.isBlank(riStakeholderCode)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termQuery("riStakeholderCode.keyword", riStakeholderCode))
        return this
    }

    BfBillQueryBuilder riStakeholderCodes(List<String> riStakeholderCodes) {
        if (MyCollectionUtils.isNullOrEmpty(riStakeholderCodes)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termsQuery("riStakeholderCode.keyword", riStakeholderCodes))
        return this
    }

    BfBillQueryBuilder policyNo(String policyNo) {
        if (MyStringUtils.isBlank(policyNo)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termQuery("policyNo.keyword", policyNo))
        return this
    }

    BfBillQueryBuilder payModes(List<String> payModes) {
        if (MyCollectionUtils.isNullOrEmpty(payModes)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termsQuery("payMode.keyword", payModes))
        return this
    }

    BfBillQueryBuilder onlyDirectDebit() {
        payModes([PAConst.PAYMODE_DIRECT_DEBIT])
    }

    BfBillQueryBuilder onlyOutstandingPaymentBills() {
        paymentStatus(BcpConst.BILL_PAY_STATUS_OUTSTANDING)
    }

    BfBillQueryBuilder paymentStatus(String paymentStatus, boolean ignoreSuspendedOfOutstanding = true) {
        if (paymentStatus == BcpConst.BILL_PAY_STATUS_FULLY_PAID) {
            return onlyFullyPaid()
        } else if (paymentStatus == BcpConst.BILL_PAY_STATUS_OUTSTANDING) {
            onlyOutstanding()

            if (ignoreSuspendedOfOutstanding) {
                suspended(false)
            } else {
                // My Account查询需要查出被bills settle操作过的, 此时suspended是true, 并且billsSettleId有值
                boolQueryBuilder.must(
                        QueryBuilders.boolQuery()
                                .should(QueryBuilders.termQuery("suspended", false))
                                .should(
                                        QueryBuilders.boolQuery()
                                                .must(QueryBuilders.termQuery("suspended", true))
                                                .must(QueryBuilders.existsQuery("billsSettleId"))
                                )
                )
            }
        } else if (paymentStatus == BcpConst.BILL_PAY_STATUS_PAYMENT_IN_PROGRESS) {
            onlyOutstanding()
            suspended(true)
        }
        return this
    }

    BfBillQueryBuilder billsSettleBound(boolean bound) {
        if (bound) {
            boolQueryBuilder.must(QueryBuilders.existsQuery("billsSettleId"))
        } else {
            boolQueryBuilder.mustNot(QueryBuilders.existsQuery("billsSettleId"))
        }
        return this
    }

    BfBillQueryBuilder suspended(boolean suspended = true) {
        boolQueryBuilder.must(QueryBuilders.termQuery("suspended", suspended))
        return this
    }

    BfBillQueryBuilder billStatus(String billStatus) {
        if (MyStringUtils.isBlank(billStatus)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termQuery("status.keyword", billStatus))
        return this
    }

    BfBillQueryBuilder drcr(Integer drcr) {
        if (drcr == null) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termQuery("drcr", drcr))
        return this
    }

    BfBillQueryBuilder billTypes(List<String> billTypes) {
        if (MyCollectionUtils.isNullOrEmpty(billTypes)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termsQuery("billType.keyword", billTypes))
        return this
    }

    BfBillQueryBuilder reinsurerCodes(List<String> reinsurerCodes) {
        if (MyCollectionUtils.isNullOrEmpty(reinsurerCodes)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termsQuery("reinsurerCode.keyword", reinsurerCodes))
        return this
    }

    BfBillQueryBuilder riBrokerCodes(List<String> riBrokerCodes) {
        if (MyCollectionUtils.isNullOrEmpty(riBrokerCodes)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termsQuery("riBrokerCode.keyword", riBrokerCodes))
        return this
    }

    BfBillQueryBuilder riTypes(List<String> riTypes) {
        if (MyCollectionUtils.isNullOrEmpty(riTypes)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termsQuery("riType.keyword", riTypes))
        return this
    }

    BfBillQueryBuilder bizTypes(List<String> bizTypes) {
        if (MyCollectionUtils.isNullOrEmpty(bizTypes)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termsQuery("bizType.keyword", bizTypes))
        return this
    }

    BfBillQueryBuilder custRelated(boolean isCustRelated, List<String> policyIds, List<String> maIds) {
        if (isCustRelated) {
            boolQueryBuilder.must(
                    QueryBuilders.boolQuery()
                            .should(QueryBuilders.termsQuery("policyId.keyword", policyIds))
                            .should(QueryBuilders.termsQuery("maId.keyword", maIds))
            )
        }
        return this
    }

    BfBillQueryBuilder ctntCode(String ctntCode) {
        if (MyStringUtils.isBlank(ctntCode)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termQuery("ctntCode.keyword", ctntCode))
        return this
    }

    BfBillQueryBuilder includeNewBusinessAndRenewal() {
        bizTypes(GlobalConst.ALL_NEW_POLICY_BIZ_TYPES)
    }

    BfBillQueryBuilder includeNewBusinessEndoAndRenewal() {
        def types = [GlobalConst.BIZ_TYPE_ENDO]
        types.addAll(GlobalConst.ALL_NEW_POLICY_BIZ_TYPES)
        bizTypes(types)
    }

    BfBillQueryBuilder onlyNB() {
        bizTypes([GlobalConst.BIZ_TYPE_NB])
    }

    BfBillQueryBuilder onlyRenewal() {
        bizTypes([GlobalConst.BIZ_TYPE_RN])
    }

    BfBillQueryBuilder onlyEndo() {
        bizTypes([GlobalConst.BIZ_TYPE_ENDO])
    }

    BfBillQueryBuilder onlyClaim() {
        bizTypes([GlobalConst.BIZ_TYPE_CLAIM])
    }

    BfBillQueryBuilder claimNo(String claimNo) {
        if (MyStringUtils.isBlank(claimNo)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termQuery("claimNo.keyword", claimNo))
        return this
    }

    BfBillQueryBuilder onlyOutstanding() {
        boolQueryBuilder.mustNot(QueryBuilders.termQuery("balance", 0))
        return this
    }

    BfBillQueryBuilder onlyFullyPaid() {
        boolQueryBuilder.must(QueryBuilders.termQuery("balance", 0))
        return this
    }

    BfBillQueryBuilder onlyValid() {
        billStatus(BcpConst.BILL_STATUS_VALID)
    }

    BfBillQueryBuilder overdueXDays(int daysMax, int daysMin) {
        return dueDateRange(new DateRange(
                from: MyDateUtils.jodaNow().minusDays(daysMax).toDate(),
                to: MyDateUtils.jodaNow().minusDays(daysMin).toDate()
        ))
    }

    BfBillQueryBuilder showZeroAmountRecords(String showZeroAmountRecords) {
        if (MyStringUtils.isBlank(showZeroAmountRecords) || showZeroAmountRecords.trim() == "N") {
            boolQueryBuilder.mustNot(QueryBuilders.termQuery("amount", 0))
        }
        return this
    }

    BfBillQueryBuilder accountManagers(List<String> accountManagers) {
        if (MyCollectionUtils.isNullOrEmpty(accountManagers)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termsQuery("scAccountManager.keyword", accountManagers))
        return this
    }

    BfBillQueryBuilder accountOwners(List<String> accountOwners) {
        if (MyCollectionUtils.isNullOrEmpty(accountOwners)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termsQuery("scAccountManager.keyword", accountOwners))
        return this
    }

    BfBillQueryBuilder policyId(String policyId) {
        if (MyStringUtils.isBlank(policyId)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termQuery("policyId.keyword", policyId))
        return this
    }

    BfBillQueryBuilder maId(String maId) {
        if (MyStringUtils.isBlank(maId)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termQuery("maId.keyword", maId))
        return this
    }

    BfBillQueryBuilder maNo(String maNo) {
        if (MyStringUtils.isBlank(maNo)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termQuery("maNo.keyword", maNo))
        return this
    }

    BfBillQueryBuilder commissionSettleId(String commissionSettleId) {
        if (MyStringUtils.isBlank(commissionSettleId)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termQuery("commissionSettleId.keyword", commissionSettleId))
        return this
    }

    BfBillQueryBuilder commissionSettleScItemId(String commissionSettleScItemId) {
        if (MyStringUtils.isBlank(commissionSettleScItemId)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termQuery("commissionSettleScItemId.keyword", commissionSettleScItemId))
        return this
    }

    BfBillQueryBuilder currencyCode(String currencyCode) {
        if (MyStringUtils.isBlank(currencyCode)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termQuery("currencyCode.keyword", currencyCode))
        return this
    }

    BfBillQueryBuilder currencyTHB() {
        return currencyCode(GlobalConst.Currency.THB)
    }

    BfBillQueryBuilder currencySGD() {
        return currencyCode(GlobalConst.Currency.SGD)
    }

    BfBillQueryBuilder defaultCurrency() {
        return currencyCode(UserUtils.getMyTenantCurrencyCode())
    }

    BfBillQueryBuilder billDateRange(DateRange range) {
        return dateRange("billDate", range)
    }

    BfBillQueryBuilder dueDateRange(DateRange range) {
        return dateRange("dueDate", range)
    }

    BfBillQueryBuilder dueDateTill(Date tillDate) {
        boolQueryBuilder.must(
                QueryBuilders.rangeQuery("dueDate")
                        .lte(MyDateUtils.format(tillDate))
                        .timeZone(TimeZoneUtils.timeZoneIdStr)
                        .format("dd/MM/yyyy||yyyy")
        )
        return this
    }

    BfBillQueryBuilder dueDateFrom(Date fromDate) {
        boolQueryBuilder.must(
                QueryBuilders.rangeQuery("dueDate")
                        .gte(MyDateUtils.format(fromDate))
                        .timeZone(TimeZoneUtils.timeZoneIdStr)
                        .format("dd/MM/yyyy||yyyy")
        )
        return this
    }

    BfBillQueryBuilder createdAtTo(Date toDate) {
        boolQueryBuilder.must(
                QueryBuilders.rangeQuery("createdAt")
                        .lte(MyDateUtils.format(toDate))
                        .timeZone(TimeZoneUtils.timeZoneIdStr)
                        .format("dd/MM/yyyy||yyyy")
        )
        return this
    }

    BfBillQueryBuilder collectDateRange(DateRange range) {
        return dateRange("collectedAt", range)
    }

    BfBillQueryBuilder fullySettledDateRange(DateRange range) {
        return dateRange("fullySettledDate", range)
    }

    BfBillQueryBuilder fromNow(String fieldName) {
        boolQueryBuilder.must(
                QueryBuilders.rangeQuery(fieldName)
                        .gte("now")
                        .timeZone(TimeZoneUtils.timeZoneIdStr)
                        .format("dd/MM/yyyy||yyyy")
        )
        return this
    }

    BfBillQueryBuilder queryOutstandingAmountAndBalanceTillNowGroupByScAndProduct(String currencyCode, int size) {
        return this.applyAuth()
                .groupByAndNCount("by_itntCode", "itntCode.keyword", size)
                .groupByAndNCount("by_scCate", "scCate.keyword", size)
                .groupByAndNCount("by_scCode", "scCode.keyword", size)
                .groupByAndNCount("by_product", "productCode.keyword", size)
                .byDateRanges("periods", "dueDate", [
                        [key: "365d+", from: null, to: "now/d-366d"],
                        [key: "365d", from: "now/d-365d", to: "now/d-181d"],
                        [key: "180d", from: "now/d-180d", to: "now/d-91d"],
                        [key: "90d", from: "now/d-90d", to: "now/d-61d"],
                        [key: "60d", from: "now/d-60d", to: "now/d-31d"],
                        [key: "30d", from: "now/d-30d", to: "now/d-1d"],
                        [key: 'notDue', from: 'now/d', to: null]
                ])
                .sumAmountAndBalance("amount_sum", "balance_sum")
                .currencyCode(currencyCode)
//                .paymentStatus(BcpConst.BILL_PAY_STATUS_OUTSTANDING, false)
                .onlyOutstanding()
//                .dueDateTill(MyDateUtils.now())
    }

    BfBillQueryBuilder dateRange(String fieldName, DateRange range) {
        if (range == null) {
            return this
        }
        if (range.from == null) {
            range.from = MyDateUtils.now()
        }
        String from = MyDateUtils.format(range.from)
        String to
        if (range.to == null) {
            to = "now"
        } else {
            to = MyDateUtils.format(range.to)
        }
        boolQueryBuilder.must(
                QueryBuilders.rangeQuery(fieldName)
                        .gte(from).lte(to)
                        .timeZone(TimeZoneUtils.timeZoneIdStr)
                        .format("dd/MM/yyyy||yyyy")
        )
        return this
    }

    BfBillQueryBuilder billedThisMonth() {
        return billDateRange(new DateRange(
                from: MyDateUtils.getFirstDayOfCurrMonth(),
                to: MyDateUtils.getLastDayOfCurrMonth()
        ))
    }

    BfBillQueryBuilder orderByLastUpdatedAtDesc() {
        ssb.sort("lastUpdatedAt", SortOrder.DESC)
        return this
    }

    BfBillQueryBuilder orderByBillDateDesc() {
        ssb.sort("billDate", SortOrder.DESC)
        return this
    }

    BfBillQueryBuilder orderByBillDateAsc() {
        ssb.sort("billDate", SortOrder.ASC)
        return this
    }

    BfBillQueryBuilder orderByPolicyNoAsc() {
        ssb.sort("policyNo.keyword", SortOrder.ASC)
        return this
    }

    BfBillQueryBuilder orderByMaNoAsc() {
        ssb.sort("maNo.keyword", SortOrder.ASC)
        return this
    }

    BfBillQueryBuilder orderByCreatedAtAsc() {
        ssb.sort("createdAt", SortOrder.ASC)
        return this
    }

    BfBillQueryBuilder dateRangeGteLt(String fieldName, DateRange range) {
        if (range == null) {
            return this
        }
        if (range.from == null) {
            range.from = MyDateUtils.now()
        }
        String from = MyDateUtils.format(range.from)
        String to
        if (range.to == null) {
            to = "now"
        } else {
            to = MyDateUtils.format(range.to)
        }
        boolQueryBuilder.must(
                QueryBuilders.rangeQuery(fieldName)
                        .gte(from).lt(to)
                        .timeZone(TimeZoneUtils.timeZoneIdStr)
                        .format("dd/MM/yyyy||yyyy")
        )
        return this
    }

    SearchResponse executeQuery() {
        if (parentAggs != null) {
            ssb.aggregation(rootAggs)
        }
        if (boolQueryBuilder != null) {
            ssb.query(boolQueryBuilder)
        }
        if (size != null && size >= 0) {
            ssb.size(size)
        } else {
            ssb.size(0)
        }
        if (from != null && from >= 0) {
            ssb.from(from)
        } else {
            ssb.from(0)
        }
        SearchRequest searchRequest = new SearchRequest()
        searchRequest.indices(ESConst.ES_INDEX_BF_BILL.indexName)
        searchRequest.source(ssb)
        logSearchRequest(searchRequest)
        SearchResponse searchResponse = ESUtils.search(searchRequest, RequestOptions.DEFAULT)
        logSearchResponse(searchResponse)
        return searchResponse
    }

    SearchResponse executeScrollQuery(Scroll scroll) {
        if (parentAggs != null) {
            ssb.aggregation(rootAggs)
        }
        if (boolQueryBuilder != null) {
            ssb.query(boolQueryBuilder)
        }
        if (size != null && size >= 0) {
            ssb.size(size)
        } else {
            ssb.size(0)
        }
        if (from != null && from >= 0) {
            ssb.from(from)
        } else {
            ssb.from(0)
        }
        SearchRequest searchRequest = new SearchRequest()
        searchRequest.indices(ESConst.ES_INDEX_BF_BILL.indexName)
        searchRequest.source(ssb)
        searchRequest.scroll(scroll)
        logSearchRequest(searchRequest)
        SearchResponse searchResponse = ESUtils.search(searchRequest, RequestOptions.DEFAULT)
        logSearchResponse(searchResponse)
        return searchResponse
    }

    BfBillQueryBuilder queryOutstandingAmountAndBalanceTillNowGroupBySc(String currencyCode, int size) {
        return this.applyAuth()
                .groupByAndNCount("by_itntCode", "itntCode.keyword", size)
                .groupByAndNCount("by_scCate", "scCate.keyword", size)
                .groupByAndNCount("by_scCode", "scCode.keyword", size)
                .byDateRanges("periods", "dueDate", [
                        [key: "365d+", from: null, to: "now/d-366d"],
                        [key: "365d", from: "now/d-365d", to: "now/d-181d"],
                        [key: "180d", from: "now/d-180d", to: "now/d-91d"],
                        [key: "90d", from: "now/d-90d", to: "now/d-61d"],
                        [key: "60d", from: "now/d-60d", to: "now/d-31d"],
                        [key: "30d", from: "now/d-30d", to: "now/d-1d"],
                        [key: 'notDue', from: 'now/d', to: null]
                ])
                .sumAmountAndBalance("amount_sum", "balance_sum")
                .currencyCode(currencyCode)
//                .paymentStatus(BcpConst.BILL_PAY_STATUS_OUTSTANDING)
//                .dueDateTill(MyDateUtils.now())
    }

    BfBillQueryBuilder itntCodes(List<String> itntCodes) {
        if (MyCollectionUtils.isNullOrEmpty(itntCodes)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termsQuery("itntCode.keyword", itntCodes))
        return this
    }
}
