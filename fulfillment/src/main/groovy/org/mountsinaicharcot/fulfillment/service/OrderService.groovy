package org.mountsinaicharcot.fulfillment.service


import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.amazonaws.services.sqs.model.Message
import com.amazonaws.services.sqs.model.ReceiveMessageRequest
import groovy.json.JsonSlurper
import groovy.transform.CompileDynamic
import groovy.transform.CompileStatic
import groovy.transform.ToString
import groovy.util.logging.Slf4j
import org.mountsinaicharcot.fulfillment.dto.OrderInfoDto
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse
import software.amazon.awssdk.utils.Pair

@Service
@Slf4j
@CompileStatic
class OrderService {
  // DynamoDB's limit on the size of each record is 400KB, but set to 300KB to allow some buffer
  private static final Integer DYNAMODB_MAX_ITEM_SIZE_IN_BYTES = 300000

  @Value('${charcot.dynamodb.order.table.name}')
  String dynamoDbOrderTableName

  @Value('${charcot.sqs.order.queue.url}')
  String sqsOrderQueueUrl

  Map<String, String> retrieveNextOrderId() {
    AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient()
    ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
      .withQueueUrl(sqsOrderQueueUrl)
      .withMaxNumberOfMessages(1)
    List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages()
    if (messages) {
      Message message = messages[0]
      return [orderId         : (new JsonSlurper().parseText(message.body.toString()) as Map<String, Object>).orderId as String,
        sqsReceiptHandle: message.receiptHandle]
    }
    null
  }

  OrderInfoDto retrieveOrderInfo(String orderId, Integer recordNumber = 0) {
    QueryRequest queryRequest = QueryRequest.builder()
      .keyConditionExpression('orderId=:orderId')
      .tableName(dynamoDbOrderTableName)
      .expressionAttributeValues([':orderId': AttributeValue.builder().s(orderId).build()])
      .build() as QueryRequest

    DynamoDbClient dynamoDB = DynamoDbClient.builder().build()
    List<Map<String, AttributeValue>> items = dynamoDB.query(queryRequest).items()

    if (!items) {
      return null
    }

    Map<String, AttributeValue> item = items.find {
      Integer.valueOf(it.recordNumber.n()) == recordNumber
    }

    OrderInfoDto orderInfoDto = new OrderInfoDto()
    orderInfoDto.orderId = orderId
    orderInfoDto.recordNumber = item.recordNumber.n().toInteger()
    orderInfoDto.fileNames = item.fileNames?.l()?.collect { it.s() }
    orderInfoDto.filesProcessed = collectFilesProcessed(items)
    orderInfoDto.email = item.email?.s()
    orderInfoDto.filter = item.filter?.s()
    orderInfoDto.outputPath = "$FulfillmentUtil.WORK_FOLDER/$orderId"
    orderInfoDto.status = item.status?.s()
    orderInfoDto.remark = item.remark?.s()
    orderInfoDto.sqsReceiptHandle = item.sqsReceiptHandle?.s()
    orderInfoDto.lastRecordNumber = items.max {
      it.recordNumber.n().toInteger()
    }.recordNumber.n().toInteger()
    orderInfoDto
  }

  void updateFilesProcessed(String orderId, List<String> files) {
    OrderInfoDto orderInfo = retrieveOrderInfo(orderId)
    Integer lastRecordNumber = orderInfo.lastRecordNumber
    OrderInfoDto orderInfoForLastRecord = orderInfo
    if (lastRecordNumber > 0) {
      orderInfoForLastRecord = retrieveOrderInfo(orderId, lastRecordNumber)
    }

    /*
     * If the added files wouldn't fit in current record, create a new one.
     */
    Pair<Integer, String> recordNumberAndUpdates =
      determineNextRecordNumberAndUpdates(orderId, lastRecordNumber, orderInfoForLastRecord.approximateItemSizeInBytes(), (orderInfoForLastRecord.filesProcessed.toSet() + files.toSet()).join(','), (files.toSet() - orderInfoForLastRecord.filesProcessed.toSet()).join(','))
    updateOrder(orderId, recordNumberAndUpdates.left(), [name: "filesProcessed", value: (recordNumberAndUpdates.right().split(',')).collect { AttributeValue.builder().s(it).build() }, dataType: "l"] as AttributeUpdateInfo)
  }

  void updateOrderStatus(String orderId, String status, String remark = null) {
    OrderInfoDto orderInfo = retrieveOrderInfo(orderId)
    status && updateOrder(orderId, orderInfo.recordNumber, [name: 'status', value: status, dataType: 's'] as AttributeUpdateInfo)
    updateOrderRemark(orderId, remark)
  }

  void cancelOrder(String orderId) {
    updateOrderStatus(orderId, 'canceled')
  }

  void failOrder(String orderId, Exception e) {
    updateOrderStatus(orderId, 'failed', e.toString())
  }

  private void finishOrder(String orderId) {
    updateOrderStatus(orderId, 'processed', "Request processed successfully on ${FulfillmentUtil.currentTime()}")
  }

  void startOrder(String orderId) {
    updateOrderStatus(orderId, 'processing', "Request $orderId began being processed by Mount Sinai Charcot on ${FulfillmentUtil.currentTime()}")
  }

  private void updateOrderRemark(String orderId, String remark) {
    if (!remark) {
      return
    }
    OrderInfoDto orderInfo = retrieveOrderInfo(orderId)
    Integer lastRecordNumber = orderInfo.lastRecordNumber
    OrderInfoDto orderInfoForLastRecord = orderInfo

    if (lastRecordNumber > 0) {
      orderInfoForLastRecord = retrieveOrderInfo(orderId, lastRecordNumber)
    }

    String delta = "[${FulfillmentUtil.currentTime()}] $remark"
    Pair<Integer, String> recordNumberAndUpdates = determineNextRecordNumberAndUpdates(orderId, lastRecordNumber, orderInfoForLastRecord.approximateItemSizeInBytes(), "$delta\n${orderInfoForLastRecord.remark ?: ''}", delta)
    updateOrder(orderId, recordNumberAndUpdates.left(), [name: 'remark', value: recordNumberAndUpdates.right(), dataType: 's'] as AttributeUpdateInfo)
  }

  void recordOrderSize(String orderId, Long size) {
    updateOrder(orderId, 0, [name: "size", value: size.toString(), dataType: "n"] as AttributeUpdateInfo)
  }

  void recordFileCount(String orderId, Integer fileCount) {
    updateOrder(orderId, 0, [name: "fileCount", value: fileCount.toString(), dataType: "n"] as AttributeUpdateInfo)
  }

  @CompileDynamic
  private void updateOrder(String orderId, Integer recordNumber, AttributeUpdateInfo update) {
    Map<String, String> expressionAttributeNames = [("#$update.name".toString()): update.name]
    Map<String, AttributeValue> expressionAttributeValues = [(":$update.name".toString()): (AttributeValue.builder()."$update.dataType"(update.value) as AttributeValue.Builder).build()]
    String updateExpression = "SET #$update.name = :$update.name"
    DynamoDbClient dynamoDB = DynamoDbClient.builder().build()
    UpdateItemResponse updateItemResponse = dynamoDB.updateItem(UpdateItemRequest.builder()
      .tableName(dynamoDbOrderTableName).updateExpression(updateExpression)
      .expressionAttributeNames(expressionAttributeNames)
      .expressionAttributeValues(expressionAttributeValues)
      .key([orderId: AttributeValue.builder().s(orderId).build(), recordNumber: AttributeValue.builder().n(recordNumber.toString()).build()]).build() as UpdateItemRequest)
    log.info "Updated request $orderId with $update, response was ${updateItemResponse.toString()}"
  }

  @ToString(includeNames = true, excludes = ['dataType'])
  private static class AttributeUpdateInfo {
    String name
    Object value
    String dataType
  }

  void updateSqsReceiptHandle(String orderId, Integer recordNumber, String sqsReceiptHandle) {
    updateOrder(orderId, recordNumber, new AttributeUpdateInfo(name: "sqsReceiptHandle", value: sqsReceiptHandle, dataType: "s"))
  }

  void performOrderProcessedActions(String orderId, boolean updateStatus = true) {
    AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient()
    // Refresh the SQS ReceiptHandle from DB for this order, in case the request took more than 12 hours to process
    // and other workers refreshed the receipt on behalf for this worker while it was busy working on the longer-than-12 hours request
    OrderInfoDto refreshedOrderInfo = retrieveOrderInfo(orderId)
    sqs.deleteMessage(sqsOrderQueueUrl, refreshedOrderInfo.sqsReceiptHandle)
    updateStatus && finishOrder(refreshedOrderInfo.orderId)
  }

  /**
   * Checks to see if the given combined updates wouldn't fit in the current DynamoDB item. If they do, then returns
   * the combined updates and current record number. Otherwise returns the additions (delta) and an incremented record number
   * to signal to caller that a new record in DynamoDB should be created and started with just the delta. Once/if that new record
   * fills up, the process repeats itself.
   *
   */
  private Pair<Integer, String> determineNextRecordNumberAndUpdates(String orderId, Integer currentRecordNumber, Integer currentOrderSize, String combinedUpdates, String delta) {
    Integer nextRecordNumber = currentRecordNumber
    Integer wouldBeOrderRecordSize = currentOrderSize + delta.getBytes('UTF-8').length
    String updates = combinedUpdates
    if ((currentOrderSize + updates.getBytes('UTF-8').length) > DYNAMODB_MAX_ITEM_SIZE_IN_BYTES) {
      ++nextRecordNumber
      updates = delta
      log.info "DynamoDB item limit of $DYNAMODB_MAX_ITEM_SIZE_IN_BYTES exceeded. Order $orderId would have size of $wouldBeOrderRecordSize, will store '$delta' to new recordNumber $nextRecordNumber"
    } else {
      log.info "DynamoDB item limit of $DYNAMODB_MAX_ITEM_SIZE_IN_BYTES NOT exceeded. Storing $updates to orderId $orderId and recordNumber $nextRecordNumber"
    }

    Pair.of(nextRecordNumber, updates)
  }

  private List<String> collectFilesProcessed(List<Map<String, AttributeValue>> items) {
    items.findResults { Map<String, AttributeValue> item ->
      item.filesProcessed?.l()?.collect { it.s() }
    }.flatten() as List<String>
  }
}
