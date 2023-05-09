package com.bytesforce.insmate.pub

import com.bytesforce.insmate.pub.utils.ESUtils
import com.bytesforce.insmate.pub.utils.TimeZoneUtils
import com.bytesforce.insmate.pub.utils.UserUtils
import com.bytesforce.pub.*
import groovy.util.logging.Slf4j
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.Scroll
import org.elasticsearch.search.aggregations.AggregationBuilder
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder

@Slf4j
class BfPgReportQueryBuilder extends BaseBfQueryBuilder {
    private SearchSourceBuilder ssb = new SearchSourceBuilder()
    private BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
    private AggregationBuilder rootAggs
    private AggregationBuilder parentAggs
    private Integer size
    private Integer from

    BfPgReportQueryBuilder() {
    }

    static BfPgReportQueryBuilder newInstance() {
        new BfPgReportQueryBuilder()
    }

    static BfPgReportQueryBuilder builder() {
        new BfPgReportQueryBuilder()
    }

    BfPgReportQueryBuilder size(Integer size) {
        this.size = size
        return this
    }

    BfPgReportQueryBuilder from(Integer from) {
        this.from = from
        return this
    }

    BfPgReportQueryBuilder groupBy(String aggsName, String fieldName, int minDocCount = 1) {
        def terms = AggregationBuilders.terms(aggsName).field(fieldName).minDocCount(minDocCount)
        if (parentAggs != null) {
            parentAggs.subAggregation(terms)
        }
        parentAggs = terms
        if (rootAggs == null) {
            rootAggs = parentAggs
        }
        return this
    }

    BfPgReportQueryBuilder count(String aggsName, String fieldName = "reqId") {
        def count = AggregationBuilders.count(aggsName).field("${fieldName}.keyword")
        if (parentAggs != null) {
            parentAggs.subAggregation(count)
        }
        parentAggs = count
        if (rootAggs == null) {
            rootAggs = parentAggs
        }
        return this
    }

    BfPgReportQueryBuilder applyAuth() {
        if (UserUtils.isPlatformUserLoggedIn()) {
            //has authority
        } else if (UserUtils.isInsurerLoggedIn()) {
            def itntCode = AppContext.instance.loginUser.tenantCode
            boolQueryBuilder.must(QueryBuilders.termQuery("itntCode.keyword", itntCode))
        } else if (UserUtils.isChannelLoggedIn()) {
            def path = AppContext.instance.loginUser.structPath
            def ctntCode = AppContext.instance.loginUser.tenantCode
            boolQueryBuilder.must(QueryBuilders.termQuery("ctntCode.keyword", ctntCode))
            boolQueryBuilder.must(QueryBuilders.matchPhrasePrefixQuery("ctntStructPath", path))
        } else if (UserUtils.isAnonymousLoggedIn()) {
            Panic.noAuth("No authority to query PgReport")
        } else {
            Panic.noAuth("No authority to query quote/policies")
        }
        return this
    }

    BfPgReportQueryBuilder key(String key) {
        if (MyStringUtils.isBlank(key)) {
            return this
        }
        boolQueryBuilder.must(queryKeyBuilder(key))
        return this
    }

    BfPgReportQueryBuilder status(String status) {
        if (MyStringUtils.isBlank(status)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termQuery("status.keyword", status))
        return this
    }

    BfPgReportQueryBuilder k1(String k1) {
        if (MyStringUtils.isBlank(k1)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termQuery("k1.keyword", k1))
        return this
    }

    BfPgReportQueryBuilder k2(String k2) {
        if (MyStringUtils.isBlank(k2)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termQuery("k2.keyword", k2))
        return this
    }

    BfPgReportQueryBuilder types(List<String> types) {
        if (MyCollectionUtils.isNullOrEmpty(types)) {
            return this
        }
        boolQueryBuilder.must(QueryBuilders.termsQuery("reportType.keyword", types))
        return this
    }


    BfPgReportQueryBuilder reportDateRange(DateRange range) {
        return dateRange("reportDate", range)
    }

    BfPgReportQueryBuilder orderByReportDateDesc() {
        ssb.sort("reportDate", SortOrder.DESC)
        return this
    }

    BfPgReportQueryBuilder orderByLastUpdatedAtDesc() {
        ssb.sort("lastUpdatedAt", SortOrder.DESC)
        return this
    }

    BfPgReportQueryBuilder dateRange(String fieldName, DateRange range) {
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
        searchRequest.indices(ESConst.ES_INDEX_BF_PG_REPORT.indexName)
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
        searchRequest.indices(ESConst.ES_INDEX_BF_PG_REPORT.indexName)
        searchRequest.source(ssb)
        searchRequest.scroll(scroll)
        logSearchRequest(searchRequest)
        SearchResponse searchResponse = ESUtils.search(searchRequest, RequestOptions.DEFAULT)
        logSearchResponse(searchResponse)
        return searchResponse
    }

}
