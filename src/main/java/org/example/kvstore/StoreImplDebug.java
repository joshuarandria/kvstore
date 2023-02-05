package org.example.kvstore;

import org.example.kvstore.cmd.Command;
import org.example.kvstore.cmd.Get;
import org.example.kvstore.cmd.Migr;
import org.example.kvstore.cmd.Put;
import org.example.kvstore.cmd.CommandFactory;
import org.example.kvstore.cmd.Reply;
import org.example.kvstore.distribution.ConsistentHash;
import org.example.kvstore.distribution.Strategy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import org.jgroups.*;

public class StoreImplDebug<K, V> extends ReceiverAdapter implements Store<K, V> {

  private String name;
  private Strategy strategy;
  private Strategy oldStrategy;
  private Map<K, V> data;
  private CommandFactory<K, V> factory;

  JChannel channel;
  ExecutorService workers;
  CompletableFuture<V> pending;
  CompletableFuture<V> pendingmigr;

  public static final boolean DEBUG = false;

  public StoreImplDebug(String name) {
    this.name = name;
  }

  public void init() throws Exception {
    if (DEBUG)
      System.out.print("init new store");
    data = new HashMap<>();
    channel = new JChannel();
    channel.setReceiver(this);
    channel.connect(name);
    if (strategy == null)
      strategy = new ConsistentHash(channel.getView());
    workers = Executors.newCachedThreadPool();
    factory = new CommandFactory<>();
    getMigradata();
  }

  @Override
  public V get(K k) {
    if (DEBUG)
      System.out.println("[get " + channel.getAddressAsString() + " ] k:" + k);
    Command<K, V> cmd = factory.newGetCmd(k);
    return execute(cmd);
    // return data.get(k);
  }

  @Override
  public V put(K k, V v) {
    if (DEBUG)
      System.out.println("[put " + channel.getAddressAsString() + " ] k:" + k + ", v:" + v);
    // V oldValue = get(k);
    // data.put(k, v);
    Command<K, V> cmd = factory.newPutCmd(k, v);
    return execute(cmd);
    // return oldValue;
  }

  @Override
  public String toString() {
    return "Store " + data.toString();
  }

  public void viewAccepted(View new_view) {
    if (DEBUG)
      System.out.println("\nviewAccepted from " + channel.getAddressAsString() + ": " + new_view);
    oldStrategy = strategy;
    strategy = new ConsistentHash(new_view);
  }

  public void getMigradata() {
    if (DEBUG)
      System.out.println("[getMigradata]");
    int size = channel.getView().size();
    Address lastNode = channel.getView().get(size - 1);
    if (DEBUG) {
      System.out.println("currentNode: " + channel.getAddressAsString());
      System.out.println("newNode: " + lastNode);
      System.out.println("size: " + size);
    }
    if (channel.getAddress().equals(lastNode) && size > 1) {
      if (DEBUG)
        System.out
            .println(
                "! ! !   I (" + channel.getAddressAsString() + ") am the new node, give me your migradata   ! ! !");
      Command<K, V> cmd = factory.newMigrCmd();
      for (Address addr : channel.getView().getMembers()) {
        if (!addr.equals(lastNode)) {
          send(addr, cmd);
        }
      }
    }
    System.out.println("\n");
  }

  public void receive(Message msg) {
    if (DEBUG)
      System.out.println("[receive in " + channel.getAddressAsString() + " ] from " +
          msg.getSrc() + ": " + msg.getObject());
    if (msg.getObject() instanceof Put || msg.getObject() instanceof Get) {
      // #### PUT & GET ####
      CmdHandler cmdHandler = new CmdHandler(msg.getSrc(), (Command<K, V>) msg.getObject());
      if (DEBUG)
        System.out.println(channel.getAddressAsString() + " submit GET/PUT work");
      workers.submit(cmdHandler);
    } else if (msg.getObject() instanceof Migr) {
      // #### MIGR ####
      CmdHandler cmdHandler = new CmdHandler(msg.getSrc(), (Command<K, V>) msg.getObject());
      if (DEBUG)
        System.out.println("submit Migr work");
      workers.submit(cmdHandler);
    } else if (msg.getObject() instanceof Reply) {
      // #### REPLY ####
      Reply<K, V> reply = (Reply<K, V>) msg.getObject();
      if (!reply.getKey().equals("migration"))
        pending.complete(reply.getValue());
    } else {
      System.out.println("Error msg.getObject() not instanceOf Get,Put,Migr or Reply");
    }
  }

  public void send(Address dst, Command<K, V> command) {
    if (DEBUG)
      System.out
          .println("[send from " + channel.getAddressAsString() + " ] to " + dst + " command " + command.toString());
    try {
      Message msg = new Message(dst, null, command);
      channel.send(msg);
    } catch (Exception e) {
    }
  }

  private synchronized V execute(Command<K, V> cmd) {
    if (DEBUG)
      System.out.println("[execute " + channel.getAddressAsString() + " ] cmd: " + cmd.toString());
    pending = new CompletableFuture<>();
    V result = null;
    V oldValue = data.get(cmd.getKey());
    Address addr = strategy.lookup(cmd.getKey());
    send(addr, cmd);
    try {
      result = pending.get();
      if (DEBUG)
        System.out.println("got result from pending " + result);
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    if (cmd instanceof Get) {
      return result;
    } else if (cmd instanceof Put) {
      return oldValue;
    } else {
      return null;
    }
  }

  private class CmdHandler implements Callable<Void> {
    Address callerAddress;
    Command<K, V> command;

    CmdHandler(Address callerAddress, Command<K, V> command) {
      this.callerAddress = callerAddress;
      this.command = command;
    }

    @Override
    public Void call() throws Exception {
      if (DEBUG) {
        System.out.println("[call from " + channel.getAddressAsString() + " ] " + command.toString());
      }
      V v = null;
      Reply<K, V> reply = null;
      if (command instanceof Put) {
        v = data.get(command.getKey());
        data.put(command.getKey(), command.getValue());
        reply = new Reply<K, V>(command.getKey(), v);
      } else if (command instanceof Get) {
        v = data.get(command.getKey());
        reply = new Reply<K, V>(command.getKey(), v);
      } else if (command instanceof Migr) {
        if (DEBUG) {
          System.out.println("Command instance of Migr");
          System.out.println(callerAddress.toString() + "'s data=" + data.toString());
        }
        List<K> ktoremove = new ArrayList<K>();
        for (K k : data.keySet()) {
          if (DEBUG) {
            System.out.println("._._._._._._    key " + k + " BEGIN    ._._._._._.");
            System.out.println("key " + k + " located in " +
                channel.getAddressAsString() + " ...");
          }
          if (oldStrategy.lookup(k) != strategy.lookup(k)) {
            if (DEBUG)
              System.out.println("                  ... should go in " + strategy.lookup(k));
            Command<K, V> cmd = factory.newPutCmd(k, data.get(k));
            execute(cmd);
            ktoremove.add(k);
          } else {
            if (DEBUG)
              System.out
                  .println(
                      "                  ... should remain in " + strategy.lookup(k) + " = " + oldStrategy.lookup(k));
          }
          if (DEBUG)
            System.out.println("._._._._._._    key " + k + " END    ._._._._._.");
        }
        for (K k : ktoremove) {
          if (DEBUG)
            System.out.println("k " + k + " removed from data in " + channel.getAddressAsString());
          data.remove(k, data.get(k));
        }
        if (DEBUG)
          System.out.println(callerAddress.toString() + "'s data=" + data.toString());
        reply = new Reply<K, V>((K) "migration", v);
      } else {
        System.out.println("error: command not of type Put/Get/Migr");
      }
      if (DEBUG) {
        System.out.println("reply to " + callerAddress + ": " + reply.toString());
      }

      send(callerAddress, reply);
      return null;
    }
  }
}