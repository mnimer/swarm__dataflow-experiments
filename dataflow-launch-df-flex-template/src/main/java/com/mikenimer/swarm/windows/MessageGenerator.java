package com.mikenimer.swarm.windows;


import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.TopicName;
import picocli.CommandLine;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * --project=<proejctid>
 * --topic=<topic>
 * --delay=250
 * --groups=c1,c2,c3
 * --payload="TemplateId: 49 MessageSize:88 Message:  | 60=20221024-13:25:41.793000000 | 5799=10001001 | 268=2 | 270_0=0.000733875 | 271_0=10359 | 48_0=481 | 83_0=2845994006 | 5796_0=20221024 | 731_0=11111010 | 279_0=2 | 269_0=W | 270_1=0.000819462 | 271_1=7530 | 48_1=896 | 83_1=3554276718 | 5796_1=20221024 | 731_1=01010110 | 279_1=5 | 269_1=6"
 */
@CommandLine.Command(name = "MessageGenerator", mixinStandardHelpOptions = true, version = "1.0.0",
        description = "Sends messages to PubSub")
public class MessageGenerator implements Callable<Integer> {

    @CommandLine.Option(names = {"-p", "--project"}, description = "Google Project ID")
    String project;

    @CommandLine.Option(names = {"-t","--topic"}, description = "Google PubSub TopicId")
    String topic;

    @CommandLine.Option(names = {"-d", "--delay"}, description = "ms delay between sending messages")
    Integer delayMs;

    @CommandLine.Option(names = {"-g", "--groups"}, description = "send multiple messages for different order keys")
    String groups;

    @CommandLine.Option(names = {"--payload"}, description = "Message Payload")
    String payload;

    @CommandLine.Option(names = "--help", usageHelp = true, description = "display this help and exit")
    boolean help;



    public static void main(String[] args) {
        new CommandLine(new MessageGenerator()).execute(args);
    }

    @Override
    public Integer call() throws Exception { // your business logic goes here...
        publishPubSubMessages();
        return 0;
    }


    public void publishPubSubMessages() {

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    // Create a publisher instance with default settings bound to the topic
                    TopicName topicName = TopicName.of(project, topic);
                    final Publisher publisher = Publisher.newBuilder(topicName).build();

                    Map<String, Long> channelCounts = new HashMap<>();
                    while (true) {

                        String[] _groups = groups.split(",");
                        for (String  group : _groups) {
                            if( !channelCounts.containsKey(group)){
                                channelCounts.put(group, 0l);
                            }

                            Long cnt = channelCounts.get(group);
                            channelCounts.put(group, cnt+1);

                            com.google.pubsub.v1.PubsubMessage pubsubMessage =
                                    com.google.pubsub.v1.PubsubMessage.newBuilder()
                                            .setData(ByteString.copyFrom(payload.getBytes()))
                                            .putAllAttributes(ImmutableMap.of("publishTs", String.valueOf(new Date().getTime()), "groupKey", group, "sequence", String.valueOf(cnt)))
                                            .build();

                            // Once published, returns a server-assigned message id (unique within the topic)
                            ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
                            String messageId = messageIdFuture.get();
                            System.out.println("Published a message with custom attributes: " + messageId);
                        }
                        Thread.sleep(delayMs);

                    }
                }catch (IOException | InterruptedException | ExecutionException ex){
                    System.out.println(ex.getMessage());
                }
            }
        }).start();
    }
}
