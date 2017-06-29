package com.oltpbenchmark.api;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import static hah.HihMessages.ReadQueryRequest;

/**
 * Created by ilvoladore on 17/06/17.
 */
public class HihConnectionActor extends AbstractActor {

    //props
    static public Props props() {
        return Props.create(HihConnectionActor.class, () -> new HihConnectionActor());
    }

    private ActorRef listener;
    private ActorSelection selection = getContext().actorSelection("akka://HihooiMiddlewareSystem@172.16.56.48:2550/user/HihooiListener");
    public HihConnectionActor(){
        //init();
    }

    //printer logger
    private LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ReadQueryRequest.class, readR ->{
                    if (selection!=null){
                        selection.forward(readR, getContext());
                    }
                    else {
                        log.info("SERVICE UNAVAILABLE");
                    }
                })
                .matchAny(any -> {
                    log.error("Received An Unknown Message" + any);
                })
                .build();
    }

}
