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
                 running = true
                }).

worker(Wrapper, Context) ->
    worker_loop(#worker{wrapper = Wrapper, context = Context}).

worker_loop(#worker{context = #context{finished = true}, 
                    wrapper = Wrapper})->
    %% we're done
    Wrapper ! finished,
    ok;
worker_loop(#worker{running = false} = State)->
    receive
        start ->
            worker_loop(State#worker{running = true})
    end;
worker_loop(#worker{context = Context,
                    running = Running,
                    wrapper = Wrapper} = State)->
    receive
        timeout ->
            io:format("@@@@ ~p got timeout ?!~n", [self()]),
            worker_loop(State);
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
    InData = get_inputs(Wrapper, Ins),
    {Results, New_context} = Body(InData, State),
    send_results(Results, Wrapper),
    New_context.

get_inputs(Wrapper, Ins) when is_list(Ins) ->
    lists:map(fun(X) -> get_input(Wrapper, X) end, Ins).

get_input(Wrapper, {In, N}) ->
    {In, get_input(Wrapper, In, N, [])};
get_input(Wrapper, In) ->
    {In, fetch_input(Wrapper, In)}.

get_input(_Wrapper, _In, 0, Res) ->
    lists:reverse(Res);
get_input(Wrapper, In, N, Res) ->
    get_input(Wrapper, In, N-1, [fetch_input(Wrapper, In)|Res]).

fetch_input(Wrapper, InputId) ->
    receive
        {InputId, Data} ->
            Wrapper ! {used, InputId},
            Data
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
                  finished = false,
                  
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

wrapper(#pipe{name=_Name}=Pipe) ->
    Self = self(),
    Worker = spawn_link(fun() ->     
                                %%register(_Name, Self),
                                worker(Self, Pipe#pipe.context) 
                        end),
    %%register(list_to_atom(atom_to_list(_Name)++"_worker"), Worker),
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
            Worker ! {B#input.name, Data},
            B2 = case (B#input.messages >=  Stop) and (B#input.running == true) of
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
            B2 = case (B#input.messages  =< Start) and (B#input.running == false) of
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
                          Fun = fun(R) -> Out#output.pid ! {data, self(), R} end,
                          lists:foreach(Fun, RL),
                          ok
                  end,
            lists:foreach(Fun, Results),
            wrapper_loop(State);
        finished ->
            wrapper_cleanup(State#wrapper{finished=true});
        
        %% flow control (out)
        stop -> 
            case length(Outputs) of
                Started ->
                    Worker ! stop;
                _ ->
                    ok
            end,
            wrapper_loop(State#wrapper{started = Started-1});
        start -> 
            case length(Outputs)-1 of
                Started ->
                    Worker ! start;
                _ ->
                    ok
            end,
            wrapper_loop(State#wrapper{started = Started+1});
        
        Msg ->
            io:format("@@@@ wrapper ~p got unknown ~p~n", [self(), Msg]),
            wrapper_loop(State)
    
    end.

wrapper_cleanup(#wrapper{outputs=Outputs}=State) ->
    receive 
        {result, Results} ->
            Fun = fun({N, RL}) ->
                          {value, Out} = lists:keysearch(N, #output.name, Outputs),
                          [Out#output.pid ! {data, self(), R} || {data, R}<-RL],
                          ok
                  end,
            lists:foreach(Fun, Results),
            wrapper_cleanup(State)
    after 100 ->
            ok
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
    %%io:format("%% ~p ~p ~p = ~p~n", [self(), AllIns, AllOuts, AllIns and AllOuts]),
    AllIns and AllOuts.

