package com.oltpbenchmark.api;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.util.Timeout;
import hah.HihMessages;
import scala.concurrent.Await;

import java.util.concurrent.TimeUnit;

import static hah.HihMessages.HihQueryResultSet;
/**
 * Created by ilvoladore on 17/06/17.
 */
public class HihActorConnection {

    private final ActorRef client;
    private Timeout timeout = new Timeout(700, TimeUnit.MILLISECONDS);

    public HihActorConnection(ActorSystem system){
        System.out.println("CREATING ACTOR FROM THREAD: " + Thread.currentThread().getName());
        this.client = system.actorOf(Props.create(HihConnectionActor.class));
    }

    public HihQueryResultSet execQuery(String q){

        HihMessages.ReadQueryRequest.Builder req =HihMessages.ReadQueryRequest.newBuilder();
        req.setQuery(q); req.setTransetid(100l);
        HihMessages.ReadQueryRequest req2send = req.build();
        scala.concurrent.Future<Object> f =  akka.pattern.Patterns.ask(client, req2send, timeout);
        try {
            HihQueryResultSet ans = (HihQueryResultSet) Await.result(f, timeout.duration());
            return ans;
        }
        catch (Exception jle){
            jle.printStackTrace();
        }
        return null;
    }
    //TODO: implement
    public void rollback(){

    }

    public void terminate(){
        //this.client.isTerminated();
        client.tell(PoisonPill.getInstance(), ActorRef.noSender());
    }

}
