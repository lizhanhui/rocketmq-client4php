#include <iostream>
#include <phpcpp.h>
#include <rocketmq/DefaultMQPushConsumer.h>
#include <rocketmq/MessageListener.h>
#include <rocketmq/Message.h>
#include <rocketmq/MessageExt.h>
#include <rocketmq/MessageQueue.h>
#include <rocketmq/PullResult.h>
#include <rocketmq/MQClientException.h>

class PhpMessage : public Php::Base {
public:
    PhpMessage(MessageExt* messageExt) {
        pMessageExt = messageExt;
    }

    Php::Value getMsgId() {
        return pMessageExt->getMsgId();
    }

    Php::Value getTopic() {
        return pMessageExt->getTopic();
    }

    Php::Value getTags() {
        return pMessageExt->getTags();
    }

    Php::Value getKeys() {
        return pMessageExt->getKeys();
    }

    Php::Value getBodyLen() {
        return pMessageExt->getBodyLen();
    }

    /**
     * return value type: char*
     */
    Php::Value getBody() {
        return pMessageExt->getBody();
    }

private:
    MessageExt* pMessageExt;
};

class PhpMessageListener : public MessageListenerConcurrently {
private:
    const Php::Value& _callback;

public:
    PhpMessageListener(const Php::Value& callback) : _callback(callback) {
    }

    virtual ConsumeConcurrentlyStatus consumeMessage(std::list<MessageExt *> &msgs,
                                                     ConsumeConcurrentlyContext &context);
};

class PhpPushConsumer : public Php::Base {

public:
    PhpPushConsumer() {
        consumer = new DefaultMQPushConsumer();
    }

    virtual ~PhpPushConsumer() {
        delete(messageListener);
        delete(consumer);
    }

    void start() {
        consumer->start();
    }

    void shutdown() {
        consumer->shutdown();
    }

    void setConsumerGroup(Php::Parameters& params) {
        if (params.size() < 1) {
            throw Php::Exception("Incorrect number of parameters assigned.");
        }

        consumer->setConsumerGroup(params[0].stringValue());
    }

    void subscribe(Php::Parameters& params) {
        if (params.size() != 3) {
            throw Php::Exception("Incorrect number of parameters passed in");
        }
        consumer->subscribe(params[0].stringValue(), params[1].stringValue());
        Php::Value* value = new Php::Value(params[2]);
        messageListener = new PhpMessageListener(*value);
        consumer->setMessageListener(messageListener);
    }

private:
    DefaultMQPushConsumer* consumer;
    MessageListener* messageListener;
    Php::Value* callback;
};


/**
 *  tell the compiler that the get_module is a pure C function
 */
extern "C" {

/**
 *  Function that is called by PHP right after the PHP process
 *  has started, and that returns an address of an internal PHP
 *  strucure with all the details and features of your extension
 *
 *  @return void*   a pointer to an address that is understood by PHP
 */
PHPCPP_EXPORT void *get_module()
{
    // static(!) Php::Extension object that should stay in memory
    // for the entire duration of the process (that's why it's static)
    static Php::Extension extension("rocketmqclient4php", "1.0");

    Php::Class<PhpMessage> phpMessage("PhpMessage");
    phpMessage.method("getMsgId", &PhpMessage::getMsgId, {});
    phpMessage.method("getTopic", &PhpMessage::getTopic, {});
    phpMessage.method("getTags", &PhpMessage::getTags, {});
    phpMessage.method("getKeys", &PhpMessage::getKeys, {});
    phpMessage.method("getBodyLen", &PhpMessage::getBodyLen, {});
    phpMessage.method("getBody", &PhpMessage::getBody, {});
    extension.add(std::move(phpMessage));

    Php::Class<PhpPushConsumer> phpPushConsumer("PhpPushConsumer");

    phpPushConsumer.method("start", &PhpPushConsumer::start, {});

    phpPushConsumer.method("shutdown", &PhpPushConsumer::shutdown, {});

    phpPushConsumer.method("setConsumerGroup", &PhpPushConsumer::setConsumerGroup,
                           {Php::ByVal("consumerGroup", Php::Type::String)});

    phpPushConsumer.method("subscribe", &PhpPushConsumer::subscribe,
                           {Php::ByVal("topic", Php::Type::String),
                            Php::ByVal("tags", Php::Type::String),
                            Php::ByVal("consumeFunction", Php::Type::Callable)});

    extension.add(std::move(phpPushConsumer));

    // return the extension
    return extension;
}
}

ConsumeConcurrentlyStatus PhpMessageListener::consumeMessage(std::list<MessageExt *> &msgs,
                                                             ConsumeConcurrentlyContext &context) {
    MessageExt* messageExt = msgs.front();
    std::cout << "Begin to consume message. msgId: " << messageExt->getMsgId() << std::endl;
    std::cout << "Test if callable: " << (_callback.isCallable() ? " true" : "false") << std::endl;
    if (!_callback.isCallable()) {
        throw Php::Exception("Callback PHP function is expected");
    }

    Php::Value param = Php::Object("PhpMessage", new PhpMessage(messageExt));
    Php::Value value;
    try {
        value = _callback(param);
    } catch (...) {
        std::cout << "Yuck! Bussiness code is buggy!" << std::endl;
    }
/*
    if (value.numericValue() > 0) {
        std::cout << "Message Consumption Failed! Retry Later." << std::endl;
        context.ackIndex = 0;
        return RECONSUME_LATER;
    }

*/
    std::cout << "Message Consumed OK" << std::endl;
    context.ackIndex = 1;
    return CONSUME_SUCCESS;
}
