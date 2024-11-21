import signal

if __name__ == '__main__':
    import sys

    from mq_http_sdk.mq_exception import MQExceptionBase
    from mq_http_sdk.mq_producer import *
    from mq_http_sdk.mq_client import *
    import time

    # 初始化client。
    mq_client = MQClient(

    )
    # 消息所属的Topic，在消息队列RocketMQ版控制台创建。
    topic_name = "ali_mq"
    # Topic所属的实例ID，在消息队列RocketMQ版控制台创建。
    # 若实例有命名空间，则实例ID必须传入；若实例无命名空间，则实例ID传入空字符串。实例的命名空间可以在消息队列RocketMQ版控制台的实例详情页面查看。
    instance_id = "MQ_INST_1371064537777105_BYypy2A8"

    producer = mq_client.get_producer(instance_id, topic_name)

    # 循环发送4条消息。
    msg_count = 1

    # signal.SIGTERM
    print("%sPublish Message To %s\nTopicName:%s\nMessageCount:%s\n" % (10 * "=", 10 * "=", topic_name, msg_count))

    try:
        for i in range(msg_count):
            msg = TopicMessage(
                # 消息内容。
                "I am test message %s.hello" % i,
                # 消息标签。
                "dish1 || dish2"
            )
            # 设置消息的自定义属性。
            msg.put_property("a", "b")
            # 设置消息的Key。
            msg.set_message_key("MessageKey")
            # msg.set_sharding_key("sharding_key")
            msg.set_start_deliver_time(int(round(time.time() * 1000)) + 10 * 1000)
            re_msg = producer.publish_message(msg)
            print("Publish Message Succeed. MessageID:%s, BodyMD5:%s" % (re_msg.message_id, re_msg.message_body_md5))

    except MQExceptionBase as e:
        if e.type == "TopicNotExist":
            print("Topic not exist, please create it.")
            sys.exit(1)
        print("Publish Message Fail. Exception:%s" % e)