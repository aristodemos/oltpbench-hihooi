package com.oltpbenchmark.api;

import akka.actor.ActorSystem;

/**
 * Created by ilvoladore on 17/06/17.
 */
public class HihActorSystem {

    private final ActorSystem system;

    //MAKE THIS A SINGLETON USING A PRIVATE CONSTRUCTOR
    private static HihActorSystem instance = null;
    private static Object mutex= new Object();
    private HihActorSystem(){
        system = ActorSystem.create("HihooiMiddlewareSystem");
    }

    public static HihActorSystem getInstance(){
        if(instance==null){
            synchronized (mutex){
                if(instance==null) instance= new HihActorSystem();
            }
        }
        return instance;
    }

    public static ActorSystem getSystem(){
        return HihActorSystem.getInstance().system;
    }

}
