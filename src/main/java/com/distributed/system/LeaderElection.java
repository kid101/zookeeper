package com.distributed.system;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {
    public static final String ZOO_KEEPER_ADDRESS ="localhost:2181";
    private ZooKeeper zooKeeper;
    public static final int ZOO_KEEPER_SESSION_TIMEOUT = 3000;
    public static final String ELECTION_NAMESPACE = "/election";
    public static final String TARGET_Z_NODE_NAMESPACE = "/target";
    public static String CURRENT_Z_NODE_PATH;
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        LeaderElection leaderElection= new LeaderElection();
        leaderElection.connectToZookeeper();
        leaderElection.volunteerForLeadership();
        leaderElection.reelectLeader();
        leaderElection.run();
        leaderElection.close();
        System.out.println("ending program");
    }
    public void connectToZookeeper() throws IOException, InterruptedException {
        this.zooKeeper= new ZooKeeper(ZOO_KEEPER_ADDRESS,ZOO_KEEPER_SESSION_TIMEOUT,this);
    }
    public  void volunteerForLeadership() throws KeeperException, InterruptedException {
        String zNodePrefix=ELECTION_NAMESPACE+"/c_";
        final String zNodeFullPath = zooKeeper.create(zNodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("created zNode full path: "+ zNodeFullPath);
        CURRENT_Z_NODE_PATH=zNodeFullPath.replace(ELECTION_NAMESPACE+"/","");
    }
    public void watchTarget() throws KeeperException, InterruptedException {
        Stat exists = zooKeeper.exists(TARGET_Z_NODE_NAMESPACE, this);
        System.out.println("exists value is: " +exists);
        if(exists!=null){
            final byte[] data = zooKeeper.getData(TARGET_Z_NODE_NAMESPACE, this, exists);
            final List<String> children = zooKeeper.getChildren(TARGET_Z_NODE_NAMESPACE, this);
            System.out.println("children present are : "+children + ", data present in target: "+ TARGET_Z_NODE_NAMESPACE+" is: "+ (data!=null?new String(data):"no data in Znode"));
        } else {
            System.out.println("nothing found yet");
        }
    }
    public void reelectLeader() throws KeeperException, InterruptedException {
        String predecessorNode=null;
        while(predecessorNode==null) {
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);
            String leader = children.get(0);
            System.out.println(leader);
            if (CURRENT_Z_NODE_PATH.equals(leader)) {
                System.out.println("I'm the leader");
                return;
            } else {
                System.out.println("I'm not the leader, the current leader is : " + leader);
                int predecessorNodeLocation = Collections.binarySearch(children, CURRENT_Z_NODE_PATH) - 1;
                predecessorNode = children.get(predecessorNodeLocation);
                zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorNode, this);
                System.out.println("we are watching :" + predecessorNode);
            }
        }
    }
    public void run() throws InterruptedException {
        synchronized (this.zooKeeper){
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        synchronized (this.zooKeeper){
            zooKeeper.close();
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
        switch (watchedEvent.getType()){
            case None:
                if(watchedEvent.getState().equals(Event.KeeperState.SyncConnected)){
                    System.out.println("successfully connected to zookeeper");
                }
                if(watchedEvent.getState().equals(Event.KeeperState.Closed)){
                    System.out.println("connection to zookeeper closed" + watchedEvent);
                    synchronized (this.zooKeeper){
                        this.zooKeeper.notifyAll();
                    }
                }
                break;
            case NodeDeleted:
                if(watchedEvent.getWrapper().getPath().contains(ELECTION_NAMESPACE)){
                    System.out.println("election node was deleted");
                    try {
                     reelectLeader();
                    } catch (InterruptedException | KeeperException e) {
                        e.printStackTrace();
                    }
                }
                break;
            case NodeCreated:
                System.out.println("some node was created: "+ watchedEvent);
                break;
            case NodeDataChanged:
                System.out.println("some node data was changed: "+ watchedEvent);
                break;
            case NodeChildrenChanged:
                System.out.println("some node children was changed: "+ watchedEvent);
                break;
            default:
                System.out.println("some event has happend: "+ watchedEvent);
                break;
        }
        try {
            this.watchTarget();
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }
}
