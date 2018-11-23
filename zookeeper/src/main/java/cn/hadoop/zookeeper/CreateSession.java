package cn.hadoop.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
* @Author  zz
* @Date  2018/11/23  10:00
* @ClassName  CreateSession
*/
public class CreateSession implements Watcher {
    private static ZooKeeper zk ;
    /**
     *  创建一个与服务器的连接 需要(服务端的 ip+端口号)(session过期时间)(Watcher监听注册)
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        zk = new ZooKeeper("localhost",2181,new CreateSession());
        System.out.println(zk.getState());
        Thread.sleep(Integer.MAX_VALUE);
    }

    private void zk(){
        System.out.println("zk");
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("Recive watched event" + event);
        if (event.getState() == Event.KeeperState.SyncConnected){
            System.out.println("ZooKeeper session established.");
           zk();
        }
    }
}
