package cn.hadoop.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ZKClient {
    public static void main(String[] args) throws IOException {
        String hostPort = "127.0.0.1:2181";
        String zpath = "/";
        List<String> zooChildren = new ArrayList<String>();
        ZooKeeper zk = new ZooKeeper(hostPort, 2000, null);
        if (zk!=null){
            try {
                zooChildren = zk.getChildren(zpath, false);
                System.out.println("Znodes of '/': ");
                for (String child: zooChildren) {
                    System.out.println(child);
                }
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
