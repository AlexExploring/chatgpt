package com.bytesforce.insmate.bcp.vo

import com.bytesforce.pub.MyESDateDeserializer
import com.bytesforce.pub.MyESDateSerializer
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize

class BillESDoc {
    //policy attributes
    String itntCode
    String ctntCode
    String policyId
    String quoteNo
    String policyNo
    String endoId
    String endoNo
    String claimId
    String claimNo
    String claimSettleId
    String claimSettleNo
    String claimRecoveryId
    String claimRecoveryNo
    String boRefNo
    String foRefNo
    String bizType
    String currencyCode
    String channelCate
    String b2b2cCate
    String clientType
    String clientOs
    String browserType
    String productCate
    String productCode
    String productVersion
    String b2cUserId
    String maId
    String maNo
    String declarationId
    String declarationNo
    String botSessionId
    @JsonSerialize(using = MyESDateSerializer)
    @JsonDeserialize(using = MyESDateDeserializer)
    Date transCreatedAt
    @JsonSerialize(using = MyESDateSerializer)
    @JsonDeserialize(using = MyESDateDeserializer)
    Date transUpdatedAt
    @JsonSerialize(using = MyESDateSerializer)
    @JsonDeserialize(using = MyESDateDeserializer)
    Date effDate
    @JsonSerialize(using = MyESDateSerializer)
    @JsonDeserialize(using = MyESDateDeserializer)
    Date expDate
    String agrmtId
    String agrmtCode
    String scCode
    String perfScCode
    String perfScCate
    String issuedBy
    @JsonSerialize(using = MyESDateSerializer)
    @JsonDeserialize(using = MyESDateDeserializer)
    Date issueDate
    @JsonSerialize(using = MyESDateSerializer)
    @JsonDeserialize(using = MyESDateDeserializer)
    Date lapseDueDate
    String prevPolicyNo
    String itntStructId
    String ctntStructId
    String itntStructPath
    String ctntStructPath
    boolean openEnd
    boolean installmentPayment
    String planCode
    String planVersion
    String planName
    String policyholderTitle
    String policyholderName
    String policyholderIdType
    String policyholderIdNo
    String policyholderTaxNo
    String policyholderTaxBranch
    String policyholderEmail
    String policyholderMobileNationCode
    String policyholderMobile
    String policyholderGender
    String policyholderDob
    String policyholderNationality
    String policyholderOccupation
    String policyholderIndustry
    String policyholderMaritalStatus
    String scAccountManager
    String scAccountOwner

    //bill attributes
    String scCate
    String billId
    String billRefNo
    String billType
    Integer billSeq
    String billingScheduleId
    boolean suspended
    @JsonSerialize(using = MyESDateSerializer)
    @JsonDeserialize(using = MyESDateDeserializer)
    Date billDate
    @JsonSerialize(using = MyESDateSerializer)
    @JsonDeserialize(using = MyESDateDeserializer)
    Date dueDate
    String payMode
    BigDecimal amount
    BigDecimal balance
    BigDecimal originalFeeAmount
    BigDecimal gstOrVatAmount
    String status
    Integer drcr
    @JsonSerialize(using = MyESDateSerializer)
    @JsonDeserialize(using = MyESDateDeserializer)
    Date billCancelledAt
    String billCancelledBy
    String billCancelledReason
    String endoType
    @JsonSerialize(using = MyESDateSerializer)
    @JsonDeserialize(using = MyESDateDeserializer)
    Date coveredPOIFrom
    @JsonSerialize(using = MyESDateSerializer)
    @JsonDeserialize(using = MyESDateDeserializer)
    Date coveredPOITo
    @JsonSerialize(using = MyESDateSerializer)
    @JsonDeserialize(using = MyESDateDeserializer)
    Date scheduledCancelPolicyDate
    BigDecimal daysBtBillDateAndCollectionDate
    String commissionStatus
    String commissionSubStatus
    String commissionType
    String commissionSettleId
    String commissionSettleScItemId

    String billsSettleId

    // payer/payee info
    String payerPayeeType
    String payerPayeePartyType
    String payerPayeeTitle
    String payerPayeeName1
    String payerPayeeName2
    String payerPayeeIdType
    String payerPayeeIdNo
    String payerPayeeTaxNo
    String payerPayeeTaxBranch
    String payerPayeeEmail
    String payerPayeeMobileNationCode
    String payerPayeeMobile
    String payerPayeeOfficeTelNationCode
    String payerPayeeOfficeTel
    String payerPayeeIndustry
    String payerPayeeMaritalStatus
    String payerPayeeGender
    @JsonSerialize(using = MyESDateSerializer)
    @JsonDeserialize(using = MyESDateDeserializer)
    Date payerPayeeDob
    String payerPayeeNationality
    String payerPayeeOccupation

    @JsonSerialize(using = MyESDateSerializer)
    @JsonDeserialize(using = MyESDateDeserializer)
    Date createdAt
    String createdBy
    @JsonSerialize(using = MyESDateSerializer)
    @JsonDeserialize(using = MyESDateDeserializer)
    Date lastUpdatedAt
    String lastUpdatedBy
    int ver

    BigDecimal totalPaidAmount
    @JsonSerialize(using = MyESDateSerializer)
    @JsonDeserialize(using = MyESDateDeserializer)
    Date fullySettledDate

    List<String> tags
    List<String> queryKeys

    String riStmtId
    String riStmtNo
    String xolPremAdjId
    String xolPremAdjNo
    String riBrokerCode
    String reinsurerCode
    String riStakeholderCode

    String coLeaderId
    String coLeaderCode
    String coFollowerId
    String coFollowerCode

    String riType
}
