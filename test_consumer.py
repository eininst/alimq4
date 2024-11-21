import time

if __name__ == '__main__':
    from mq_http_sdk.mq_exception import MQExceptionBase
    from mq_http_sdk.mq_consumer import *
    from mq_http_sdk.mq_client import *

    # 初始化client。
    mq_client = MQClient(
    )
    # 消息所属的Topic，在消息队列RocketMQ版控制台创建。
    topic_name = "ali_mq"
    # 您在消息队列RocketMQ版控制台创建的Group ID。
    group_id = "GID_ALI_MQ"
    # Topic所属的实例ID，在消息队列RocketMQ版控制台创建。
    # 若实例有命名空间，则实例ID必须传入；若实例无命名空间，则实例ID传入空字符串。实例的命名空间可以在消息队列RocketMQ版控制台的实例详情页面查看。
    instance_id = "MQ_INST_1371064537777105_BYypy2A8"

    consumer = mq_client.get_consumer(instance_id, topic_name, group_id)

    # 长轮询表示如果Topic没有消息，则客户端请求会在服务端挂起3秒，3秒内如果有消息可以消费则立即返回响应。
    # 长轮询时间3秒（最多可设置为30秒）。
    wait_seconds = 10
    # 一次最多消费3条（最多可设置为16条）。
    batch = 3
    print(("%sConsume And Ak Message From Topic%s\nTopicName:%s\nMQConsumer:%s\nWaitSeconds:%s\n" \
           % (10 * "=", 10 * "=", topic_name, group_id, wait_seconds)))
    while True:
        try:
            # 长轮询消费消息。
            recv_msgs = consumer.consume_message(batch, wait_seconds)
            for msg in recv_msgs:
                print(("Receive, MessageId: %s\nMessageBodyMD5: %s \
                                  \nMessageTag: %s\nConsumedTimes: %s \
                                  \nPublishTime: %s\nBody: %s \
                                  \nNextConsumeTime: %s \
                                  \nReceiptHandle: %s \
                                  \nProperties: %s\n" % \
                       (msg.message_id, msg.message_body_md5,
                        msg.message_tag, msg.consumed_times,
                        msg.publish_time, msg.message_body,
                        msg.next_consume_time, msg.receipt_handle, msg.properties)))
        except MQExceptionBase as e:
            # Topic中没有消息可消费。
            if e.type == "MessageNotExist":
                # print(("No new message! RequestId: %s" % e.req_id))
                continue

            print(("Consume Message Fail! Exception:%s\n" % e))
            time.sleep(2)
            continue

        # msg.next_consume_time前若不确认消息消费成功，则消息会被重复消费。
        # 消息句柄有时间戳，同一条消息每次消费拿到的都不一样。
        try:
            receipt_handle_list = [msg.receipt_handle for msg in recv_msgs]
            consumer.ack_message(receipt_handle_list)
            print(("Ak %s Message Succeed.\n\n" % len(receipt_handle_list)))
        except MQExceptionBase as e:
            print(("\nAk Message Fail! Exception:%s" % e))
            # 某些消息的句柄可能超时，会导致消息消费状态确认不成功。
            if e.sub_errors:
                for sub_error in e.sub_errors:
                    print(("\tErrorHandle:%s,ErrorCode:%s,ErrorMsg:%s" % \
                           (sub_error["ReceiptHandle"], sub_error["ErrorCode"], sub_error["ErrorMessage"])))