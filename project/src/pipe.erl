-module(pipe).

-export([new/1]).

%-compile([export_all]).

new(Opts) ->
    spawn(fun() -> new_pipe(Opts) end).

new_pipe(Opts) ->
    Receiver = spawn(fun() -> receiver(Opts) end),
    Sender = spawn(fun() -> sender(Receiver, Opts) end),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(sender, {
                 receiver,
                 body=fun(X) -> X end,
                 outputs=[]
                }).

sender(Receiver, Opts) ->
    sender_loop(sender_state(Receiver, Opts)).

sender_state(Receiver, _Opts) ->
    #sender{receiver=Receiver}.

sender_loop(#sender{receiver=Receiver,
                    body=Body,
                    outputs=Outs}=State)->
    receive
        {config, Cmd, Args} ->
            State1 = sender_config(State, Cmd, Args),
            sender_loop(State1)
    end.

sender_config(State, _Cmd, _Args) ->
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
                {fetch, Sender} when Size=/=0 ->
                    Sender ! hd(Buffer),
                    receiver_loop(State#receiver{buffer=tl(Buffer), buffer_size=Size-1});
                {fetch, Sender} when Size==0 ->
                    receiver_loop(State#receiver{fetch_req=FReq+1, fetch=[Fetch|Sender]})
            end
    end.

receiver_hold(State) ->
    receiver_loop(State).

receiver_config(State, _Cmd, _Args) ->
    State.



