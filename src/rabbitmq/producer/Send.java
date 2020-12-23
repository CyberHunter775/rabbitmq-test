package rabbitmq.producer;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import rabbitmq.Const;

/**
 * 生产者
 *
 */
public class Send {

    public static void main(String[] args) throws Exception {
        send();
    }

    private static void send() throws Exception {
        /**
         * 创建连接连接到MabbitMQ
         */
        ConnectionFactory factory = new ConnectionFactory();
        // 设置MabbitMQ所在主机ip或者主机名
        factory.setHost(Const.host);
        factory.setVirtualHost(Const.virtualhost);
        factory.setUsername(Const.username);
        factory.setPassword(Const.password);
        factory.setPort(Const.port);
        // 创建一个连接
        Connection connection = factory.newConnection();
        // 创建一个频道
        Channel channel = connection.createChannel();
        // 指定一个队列
        channel.queueDeclare(Const.QUEUE_NAME, false, false, false, null);
        // 时间格式化
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        int ttt = 0;
        while (true) {
            ttt++;
            InputStream is = null;
            int i = 0;
            try {
                File file = new File(Const.filepath);
                byte[] bytes = new byte[128 * 1024];
                is = new FileInputStream(file);
                int data = -1;
                String uuid = UUID.randomUUID().toString();
                System.out.println("开始发送：" + sdf.format(new Date()) + " " + uuid);
                while ((data = is.read(bytes)) != -1) {
                    String msg = new String(bytes, 0, data);
                    channel.basicPublish("", Const.QUEUE_NAME, null, msg.getBytes());
                }
                String aaaString = "1111";
                channel.basicPublish("", Const.QUEUE_NAME, null, aaaString.getBytes());
                System.out.println("发送结束：" + sdf.format(new Date()) + " " + uuid);
                is.close();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            Thread.sleep(3000);
            if (ttt == 10) {
                break;
            }
        }

        // 关闭频道和连接
        channel.close();
        connection.close();
    }

//    private static String generalStr() throws Exception {
//        String pathname = "D://test//test.txt";
//        File file = new File(pathname);
//        FileReader fileReader = new FileReader(file);
//        BufferedReader bufr = new BufferedReader(fileReader);
//        StringBuffer buffer = new StringBuffer();
//        String line = "";
//        while ((line = bufr.readLine()) != null) {
//            buffer.append(line);
//        }
//        return buffer.toString();
//    }

}
