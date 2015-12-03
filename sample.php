<?php
    function consume($msg) {
        echo "PHP Code: ";
        echo $msg->getTopic();
        echo $msg->getMsgId();
        return 0;
    }

    $phpConsumer = new PhpPushConsumer();
    $consumerGroupName = "CG_PHP";
    $topic = "TopicTest";
    $tags = "*";
    $phpConsumer->setConsumerGroup($consumerGroupName);
    $phpConsumer->subscribe($topic, $tags, "consume");
    $phpConsumer->start();


    sleep(2147483647);
?>