%% Distributed under the simplified BSD license. See enclosed LICENSE.TXT

-module(pipe).

-export([
         start/1,
         start_link/1
        ]).

%% API

-include("pipes.hrl").

start(Pipe) when is_record(Pipe, pipe) ->
    spawn(fun() -> wrapper(Pipe) end).

start_link(Pipe) when is_record(Pipe, pipe) ->
    spawn_link(fun() -> wrapper(Pipe) end).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Implementation

%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% worker %%%

-record(worker, {
                 wrapper,
                 context=#context{},
                 running = false
                }).

worker(Wrapper, Context) ->
    worker_loop(#worker{wrapper = Wrapper, context = Context}).

worker_loop(#worker{context = #context{finished = true}, 
                    wrapper = Wrapper})->
    %% we're done
    Wrapper ! finished,
    ok;
worker_loop(#worker{context = Context,
                    running = Running,
                    wrapper = Wrapper} = State)->
    receive
        stop ->
            worker_loop(State#worker{running = false});
        start ->
            worker_loop(State#worker{running = true})
    after 0 ->
            Context2 = case Running of
                           true ->
                               execute(Context, Wrapper);
                           false ->
                               State#worker.context
                       end,
            worker_loop(State#worker{context = Context2})
    end.

execute(#context{body=Body, inputs=Ins, state=State}=_Ctx, Wrapper) ->
    io:format("~p execute1 ~p~n", [self(), _Ctx]),
    InData = get_inputs(Wrapper, Ins),
    io:format("~p execute2 ~p~n", [self(), {Ins, InData, State}]),
    {Results, New_context} = Body(InData, State),
    send_results(Results, Wrapper),
    New_context.

get_inputs(Wrapper, Ins) when is_list(Ins) ->
    lists:map(fun(X) -> get_input(Wrapper, X) end, Ins).

get_input(Wrapper, In) when is_atom(In) ->
    {In, fetch_input(Wrapper, In)};
get_input(Wrapper, {In, N}) ->
    {In, get_input(Wrapper, In, N, [])}.

get_input(_Wrapper, _In, 0, Res) ->
    lists:reverse(Res);
get_input(Wrapper, In, N, Res) ->
    get_input(Wrapper, In, N-1, [fetch_input(Wrapper, In)|Res]).

fetch_input(Wrapper, InputId) ->
    receive
        {InputId, Data} ->
            Wrapper ! {used, InputId},
            Data;
        Msg ->
            io:format("!!! ~p ~p~n", [self(), Msg])
    end.

send_results([], _) ->
    ok;
send_results(Results, Wrapper) ->
    Wrapper ! {result, Results},
    ok.

%%%%%%%%%%%%%%%%
%%% wrapper %%%

-record(wrapper, {
                  connected = false,
                  worker,
                  
                  inputs = [],
                  stop_limit = 100,
                  start_limit = 1,
                  
                  outputs = [],
                  started = 0
                 }).

-record(input, {
                pid,
                name,
                messages = 0,
                running = false
               }).

-record(output, {
                 pid,
                 name
                }).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

wrapper(Pipe) ->
    Worker = spawn_link(fun() -> worker(self(), Pipe#pipe.context) end),
    wrapper_loop(wrapper_state(Worker, Pipe)).

wrapper_state(Worker, #pipe{inputs=Ins, outputs=Outs}) ->
    Inputs = fill(Ins, #input{}),
    Outputs = fill(Outs, #output{}),
    #wrapper{
             worker = Worker,
             inputs = Inputs,
             outputs = Outputs
            }.

fill(List, Rec) ->
    L0 = lists:duplicate(length(List), Rec),
    L1 = [setelement(3, R, N) || {R, N} <- lists:zip(L0, List)],
    L1.


wrapper_loop(#wrapper{
                      connected = false,
                      inputs = Ins,
                      outputs = Outs
                     } = State) ->
    receive
        {input, Name, Pid} ->
            NewIns = lists:map(fun(X) -> fix_input(X, Name, Pid) end, Ins),
            Pid ! start,
            State1 = State#wrapper{inputs=NewIns},
            wrapper_loop(State1#wrapper{connected=all_connected(State1)});
        {output, Name, Pid} ->
            NewOuts = lists:map(fun(X) -> fix_output(X, Name, Pid) end, Outs),
            State1 = State#wrapper{outputs=NewOuts},
            wrapper_loop(State1#wrapper{connected=all_connected(State1)})
    end;
wrapper_loop(#wrapper{
                      connected = true,
                      worker = Worker,
                      inputs = Inputs,
                      start_limit = Start,
                      stop_limit = Stop,
                      started = Started,
                      outputs = Outputs
                     } = State) ->
    receive
        {status, From} ->
            From ! State,
            wrapper_loop(State);
        
        %% input
        {data, From, Data} ->
            {value, B, BRest} = lists:keytake(From, #input.pid, Inputs),
            B1 = B#input{messages = B#input.messages+1},
            worker ! {B#input.name, Data},
            B2 = case (B#input.messages >=  Stop) and (B#input.running ==  true) of
                     true ->
                         B#input.pid ! stop,
                         B1#input{running = false};
                     false ->
                         B1
                 end,
            wrapper_loop(State#wrapper{inputs = [B2|BRest]});
        {used, Input} ->
            {value, B, BRest} = lists:keytake(Input, #input.name, Inputs),
            B1 = B#input{messages = B#input.messages-1},
            B2 = case (B#input.messages  =< Start) and (B#input.running ==  false) of
                     true ->
                         B#input.pid ! start,
                         B1#input{running = true};
                     false ->
                         B1
                 end,
            wrapper_loop(State#wrapper{inputs = [B2|BRest]});
        
        %%output
        {result, Results} ->
            Fun = fun({N, RL}) ->
                          {value, Out} = lists:keysearch(N, #output.name, Outputs),
                          [Out#output.pid ! {data, self(), R} || R<-RL],
                          ok
                  end,
            lists:foreach(Fun, Results),
            wrapper_loop(State);
        
        %% flow control (out)
        stop -> %%??
            case length(Outputs) of
                Started ->
                    Worker ! stop
            end,
            wrapper_loop(State#wrapper{started = Started-1});
        start -> %%??
            case length(Outputs)-1 of
                Started ->
                    Worker ! start
            end,
            wrapper_loop(State#wrapper{started = Started+1})
    
    end.

fix_input(#input{name=Name}=X, Name, Pid) ->
    X#input{pid=Pid};
fix_input(X, _Name, _Pid) ->
    X.

fix_output(#output{name=Name}=X, Name, Pid) ->
    X#output{pid=Pid};
fix_output(X, _Name, _Pid) ->
    X.

all_connected(#wrapper{inputs=Ins, outputs=Outs}) ->
    AllIns = lists:all(fun(#input{pid=Pid}) -> is_pid(Pid) end, Ins),    
    AllOuts = lists:all(fun(#output{pid=Pid}) -> is_pid(Pid) end, Outs),
    AllIns and AllOuts.

