package com.distributed.system;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {
    public static final String ZOO_KEEPER_ADDRESS ="localhost:2181";
    private ZooKeeper zooKeeper;
    public static final int ZOO_KEEPER_SESSION_TIMEOUT = 3000;
    public static final String ELECTION_NAMESPACE = "/election";
    public static String CURRENT_Z_NODE_PATH;
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        LeaderElection leaderElection= new LeaderElection();
        leaderElection.connectToZookeeper();
        leaderElection.volunteerForLeadership();
        leaderElection.electLeader();
        leaderElection.run();
        leaderElection.close();
        System.out.println("ending program");
    }
    public void connectToZookeeper() throws IOException, InterruptedException {
        this.zooKeeper= new ZooKeeper(ZOO_KEEPER_ADDRESS,ZOO_KEEPER_SESSION_TIMEOUT,this);
    }
    public  void volunteerForLeadership() throws KeeperException, InterruptedException {
        String zNodePrefix=ELECTION_NAMESPACE+"/C";
        final String zNodeFullPath = zooKeeper.create(zNodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("created zNode full path: "+ zNodeFullPath);
        CURRENT_Z_NODE_PATH=zNodeFullPath.replace(ELECTION_NAMESPACE+"/","");
    }
    public void electLeader() throws KeeperException, InterruptedException {
        List<String> children= zooKeeper.getChildren(ELECTION_NAMESPACE,false);
        Collections.sort(children);
        String leader=children.get(0);
        System.out.println(leader);
        if(CURRENT_Z_NODE_PATH.equals(leader)){
            System.out.println("I'm the leader");
        }else {
            System.out.println("I'm not the leader, the current leader is : "+leader);
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
                System.out.println("some node was deleted: "+ watchedEvent);
        }
    }
}
