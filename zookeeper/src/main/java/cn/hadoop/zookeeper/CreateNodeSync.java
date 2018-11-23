package cn.hadoop.zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 *  同步创建节点
 * @Author zz
 * @Date 2018/11/23 10:07
 * @ClassName CreateNodeSync
 */
public class CreateNodeSync implements Watcher{
    private static ZooKeeper zk ;
    public static void main(String[] args) throws IOException, InterruptedException {
        zk = new ZooKeeper("localhost:2181",5000,new CreateSession());
        System.out.println(zk.getState());
        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * 创建节点 Znode
     *  CreateMode:
     *   PERSISITENT (持续的，相当于EPHEMERAL,不会随client的断开而消失)
     *   PERSISTENT_SEQUENTIAL（持续且带顺序的）
     *   EPHEMERAL（短暂的，生命周期依赖于client session）
     *   EPHEMERAL_SEQUENTIAL（短暂的，带顺序的）
     */
    private void create() throws KeeperException, InterruptedException {
        zk.create("/node_1","123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    @Override
    public void process(WatchedEvent event) {
        System.out.println("Receive watched event:" + event);
        if(event.getState() == Event.KeeperState.SyncConnected){
            try {
                create();
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
