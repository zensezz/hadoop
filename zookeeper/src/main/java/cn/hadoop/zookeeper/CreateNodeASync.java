package cn.hadoop.zookeeper;

import org.apache.zookeeper.*;
import java.io.IOException;
/**
 *  异步创建节点
 * @Author zz
 * @Date 2018/11/23 002310:21
 * @ClassName CreateNodeASync
 */
public class CreateNodeASync implements Watcher {

    private static ZooKeeper zookeeper;

    /**
     * 创建一个与服务器的连接 需要(服务端的 ip+端口号)(session过期时间)(Watcher监听注册)
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, InterruptedException {

        zookeeper = new ZooKeeper("127.0.0.1:2181", 5000, new CreateNodeASync());
        System.out.println(zookeeper.getState());
        Thread.sleep(Integer.MAX_VALUE);
    }
    private void create() {
        zookeeper.create("/node_1", "123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                new IStringCallback(), "this is content");
        System.out.println("create node");
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("Receive watched event:" + event);
        if (event.getState() == Event.KeeperState.SyncConnected) {
            create();
        }
    }

    static class IStringCallback implements AsyncCallback.StringCallback {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            System.out.println("rc:" + rc);
            System.out.println("path:" + path);
            System.out.println("ctx:" + ctx);
            System.out.println("name:" + name);

        }
    }
}
