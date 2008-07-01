-module(pipe).

-export([new/1]).

%-compile([export_all]).

new(Opts) ->
    Receiver = spawn(fun() -> receiver(Opts) end),
    Processor = spawn(fun() -> processor(Receiver, Opts) end),
    {Receiver, Processor}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(processor, {
                    receiver,
                    body=fun(X, S) -> {X, S} end,
                    outputs=[],
                    fetch=false,
                    results=[]
                   }).

processor(Receiver, Opts) ->
    Receiver ! {config, processor, self()},
    processor_stopped(processor_state(Receiver, Opts)).

processor_state(Receiver, _Opts) ->
    #processor{receiver=Receiver}.

processor_stopped(#processor{}=State)->
    receive
        {config, Cmd, Args} ->
            State1 = processor_config(State, Cmd, Args),
            processor_stopped(State1);
        start ->
            processor_running(State)
    end.

processor_running(#processor{receiver=Receiver,
                             body=Body,
                             outputs=_Outs,
                             fetch=Fetch}=State)->
    receive
        {config, Cmd, Args} ->
            State1 = processor_config(State, Cmd, Args),
            processor_running(State1);
        stop ->
            processor_stopped(State);
        {data, Data} ->
            {Results, State1} = Body(Data, State),
            processor_send(State1#processor{fetch=false, results=Results})
    after 1 ->
            case Fetch of
                true ->
                    processor_running(State);
                false ->
                    Receiver ! fetch,
                    processor_running(State#processor{fetch=true})
            end
    end.

processor_config(State, _Cmd, _Args) ->
    State.

%%%%%%%%%%%%%%%%

-record(receiver, {
                   buffer=[],
                   buffer_size=0,
                   buffer_max=10,
                   fetch_req=0,
                   fetch=[]
                  }).

receiver(Opts) ->
    receiver_loop(receiver_state(Opts)).

receiver_state(_Opts) ->
    #receiver{}.

receiver_loop(#receiver{buffer_size=Size,
                        buffer=Buffer,
                        buffer_max=Max,
                        fetch_req=FReq,
                        fetch=Fetch}=State) ->
    case  Size > Max of
        true ->
            receiver_hold(State);
        false ->
            receive
                {config, Cmd, Args} ->
                    State1 = receiver_config(State, Cmd, Args),
                    receiver_loop(State1);
                {data, _Data}=Msg when FReq==0 ->
                    Buffer1 = [Buffer | Msg],
                    receiver_loop(State#receiver{buffer=Buffer1, buffer_size=Size+1});
                {data, _Data}=Msg when FReq=/=0 ->
                    hd(Fetch) ! Msg,
                    receiver_loop(State#receiver{fetch_req=FReq-1, fetch=tl(Fetch)});
                {fetch, processor} when Size=/=0 ->
                    processor ! hd(Buffer),
                    receiver_loop(State#receiver{buffer=tl(Buffer), buffer_size=Size-1});
                {fetch, processor} when Size==0 ->
                    receiver_loop(State#receiver{fetch_req=FReq+1, fetch=[Fetch|processor]})
            end
    end.

receiver_hold(State) ->
    receiver_loop(State).

receiver_config(State, _Cmd, _Args) ->
    State.



