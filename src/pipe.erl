-module(pipe).

-export([new/1]).

%-compile([export_all]).

new(Opts) ->
    Receiver = spawn(fun() -> receiver(Opts) end),
    Worker = spawn(fun() -> worker(Receiver, Opts) end),
    {Receiver, Worker}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(worker, {
                 receiver,
                 body=fun(X, S) -> {X, S} end,
                 outputs=[],
                 running=false,
                 results=[]
                }).

worker(Receiver, Opts) ->
    Receiver ! {config, worker, self()},
    worker_loop(worker_state(Receiver, Opts)).

worker_state(Receiver, _Opts) ->
    #worker{receiver=Receiver}.

worker_loop(#worker{receiver=_Receiver,
                    body=_Body,
                    outputs=_Outs,
                    running=Running}=State)->
    State2 = case Running of
                 true ->
                     execute(State);
                 false ->
                     State
             end,
    receive
        {config, Cmd, Args} ->
            State1 = worker_config(State2, Cmd, Args),
            worker_loop(State1);
        stop ->
            worker_loop(State2#worker{running=false});
        start ->
            worker_loop(State2#worker{running=true})
    end.

worker_config(State, _Cmd, _Args) ->
    State.

execute(State) ->
    State.

%%%%%%%%%%%%%%%%

-record(receiver, {
                   buffer_data = [],
                   stop_limit = 100,
                   start_limit = 0,
                   worker
                  }).
-record(recbuffer, {
                    pid,
                    index,
                    count=0,
                    running=false
                   }).

receiver(Opts) ->
    receiver_loop(receiver_state(Opts)).

get_opt(Name, Opts) ->
    {value, Val} = lists:keysearch(Name, 1, Opts),
    Val.

receiver_state(Opts) ->
    BL = get_opt(buffers, Opts),
    Buffers = lists:duplicate(lists:length(BL), #recbuffer{}),
    Buffers1 = [R#recbuffer{index=I} || {R, I} <- lists:zip(Buffers, lists:seq(lists:length(Buffers)))],
    #receiver{
              buffer_data = Buffers1
             }.

receiver_loop(#receiver{
                        buffer_data = Buffers,
                        start_limit=Start,
                        stop_limit=Stop,
                        worker=worker
                       }=State) ->
    receive
        {config, Cmd, Args} ->
            State1 = receiver_config(State, Cmd, Args),
            receiver_loop(State1);
        {data, From, Data} ->
            {value, B, BRest} = lists:keytake(From, #recbuffer.pid, Buffers),
            B1 = B#recbuffer{count=B#recbuffer.count+1},
            worker ! {data, B#recbuffer.index, Data},
            B2 = case (B#recbuffer.count >= Stop) and (B#recbuffer.running == true) of
                     true ->
                         B#recbuffer.pid ! stop,
                           B1#recbuffer{running=false};
                     false ->
                         B1
                 end,
            receiver_loop(State#receiver{buffer_data=[B2|BRest]});
        {fetch, Input} ->
            {value, B, BRest} = lists:keytake(Input, #recbuffer.index, Buffers),
            B1 = B#recbuffer{count=B#recbuffer.count-1},
            B2 = case (B#recbuffer.count =< Start) and (B#recbuffer.running == false) of
                     true ->
                         B#recbuffer.pid ! start,
                           B1#recbuffer{running=true};
                     false ->
                         B1
                 end,
            receiver_loop(State#receiver{buffer_data=[B2|BRest]})
    end.

receiver_config(State, _Cmd, _Args) ->
    State.



