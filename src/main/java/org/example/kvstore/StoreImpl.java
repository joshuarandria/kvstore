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

public class StoreImpl<K, V> extends ReceiverAdapter implements Store<K, V> {

  private String name;
  private Strategy strategy;
  private Strategy oldStrategy;
  private Map<K, V> data;
  private CommandFactory<K, V> factory;

  JChannel channel;
  ExecutorService workers;
  CompletableFuture<V> pending;
  CompletableFuture<V> pendingmigr;

  public StoreImpl(String name) {
    this.name = name;
  }

  public void init() throws Exception {
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
    Command<K, V> cmd = factory.newGetCmd(k);
    return execute(cmd);
  }

  @Override
  public V put(K k, V v) {
    Command<K, V> cmd = factory.newPutCmd(k, v);
    return execute(cmd);
  }

  @Override
  public String toString() {
    return "Store " + data.toString();
  }

  public void viewAccepted(View new_view) {
    oldStrategy = strategy;
    strategy = new ConsistentHash(new_view);
  }

  public void getMigradata() {
    int size = channel.getView().size();
    Address lastNode = channel.getView().get(size - 1);
    if (channel.getAddress().equals(lastNode) && size > 1) {
      Command<K, V> cmd = factory.newMigrCmd();
      for (Address addr : channel.getView().getMembers()) {
        if (!addr.equals(lastNode)) {
          send(addr, cmd);
        }
      }
    }
  }

  public void receive(Message msg) {
    if (msg.getObject() instanceof Put || msg.getObject() instanceof Get) {
      CmdHandler cmdHandler = new CmdHandler(msg.getSrc(), (Command<K, V>) msg.getObject());
      workers.submit(cmdHandler);
    } else if (msg.getObject() instanceof Migr) {
      CmdHandler cmdHandler = new CmdHandler(msg.getSrc(), (Command<K, V>) msg.getObject());
      workers.submit(cmdHandler);
    } else if (msg.getObject() instanceof Reply) {
      Reply<K, V> reply = (Reply<K, V>) msg.getObject();
      if (!reply.getKey().equals("migration"))
        pending.complete(reply.getValue());
    } else {
      System.out.println("Error msg.getObject() not instanceOf Get,Put,Migr or Reply");
    }
  }

  public void send(Address dst, Command<K, V> command) {
    try {
      Message msg = new Message(dst, null, command);
      channel.send(msg);
    } catch (Exception e) {
    }
  }

  private synchronized V execute(Command<K, V> cmd) {
    pending = new CompletableFuture<>();
    V result = null;
    V oldValue = data.get(cmd.getKey());
    Address addr = strategy.lookup(cmd.getKey());
    send(addr, cmd);
    try {
      result = pending.get();
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
        List<K> ktoremove = new ArrayList<K>();
        for (K k : data.keySet()) {
          if (oldStrategy.lookup(k) != strategy.lookup(k)) {
            Command<K, V> cmd = factory.newPutCmd(k, data.get(k));
            execute(cmd);
            ktoremove.add(k);
          }}
        for (K k : ktoremove) {
          data.remove(k, data.get(k));
        }
        reply = new Reply<K, V>((K) "migration", v);
      } else {
        System.out.println("error: command not of type Put/Get/Migr");
      }

      send(callerAddress, reply);
      return null;
    }
  }
}