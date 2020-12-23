package rabbitmq.consumer;

import java.text.SimpleDateFormat;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import rabbitmq.Const;

/**
 * 消费者
 */
public class Recv {

    // @ 结束
    // # 开始
    public static void main(String[] args) throws Exception {
        receice();
    }

    private static void receice() throws Exception {
        try {

            // 创建连接工厂
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(Const.host);
            factory.setVirtualHost(Const.virtualhost);
            factory.setUsername(Const.username);
            factory.setPassword(Const.password);
            factory.setPort(Const.port);
            // 打开连接和创建频道，与发送端一样
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            // 声明队列，主要为了防止消息接收者先运行此程序，队列还不存在时创建队列。
            channel.queueDeclare(Const.QUEUE_NAME, false, false, false, null);
            // 创建队列消费者

            System.out.println(" 开始创建消息队列消费者 ------------->>>");
            // 指定消费队列

            channel.basicQos(1);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            Consumer consumer = new DefaultConsumer(channel) {
                int aa = 0;
                long bb = 0;

                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                        byte[] body) {
                    if (aa == 0) {
                        bb = System.currentTimeMillis();
                        aa = 1;
                    }
                    if (body.length == 4) {
                        long cc = System.currentTimeMillis();
                        System.out.println("时间:" + (cc - bb));
                        aa = 0;
                        bb = 0;
                    }
                }
            };
            // 自动回复队列应答 -- RabbitMQ中的消息确认机制
            channel.basicConsume(Const.QUEUE_NAME, true, consumer);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

//        QueueingConsumer consumer = new QueueingConsumer(channel);
//        channel.basicConsume(QUEUE_NAME, true, consumer);
//        while (true) {
//            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
//            
//            String message = new String(delivery.getBody());
//            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//            
//            if (message.startsWith("#")) {
//                System.out.println("数据接收开始:" + sdf.format(new Date()));
//            } else if (message.endsWith("@")) {
//                System.out.println("数据接收结束:" + sdf.format(new Date()));
//            }
//        }
    }
}
