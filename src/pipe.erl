%% Distributed under the simplified BSD license. See enclosed LICENSE.TXT

-module(pipe).

-export([
         start/1,
         start_link/1
        ]).

%% API

start(Opts) ->
    spawn(fun() -> wrapper(Opts) end).

start_link(Opts) ->
    spawn_link(fun() -> wrapper(Opts) end).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Implementation
%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%% worker %%%

-include("pipes.hrl").

-record(worker_state, {
                       wrapper,
                       funstate = #funstate{},
                       running = false
                      }).

worker(Wrapper, Opts) ->
    worker_loop(worker_state(Wrapper, Opts)).

worker_state(Wrapper, Opts) ->
    FunState = pipes_util:get_opt(funstate, Opts),
    #worker_state{wrapper = Wrapper, funstate = FunState}.

worker_loop(#worker_state{funstate=#funstate{finished=true}})->
    ok;
worker_loop(#worker_state{funstate = FunState,
                          running = Running,
                          wrapper = Wrapper} = State)->
    receive
        {config, Cmd, Args} ->
            State1 = worker_config(State, Cmd, Args),
            worker_loop(State1);
        stop ->
            worker_loop(State#worker_state{running = false});
        start ->
            worker_loop(State#worker_state{running = true})
    after 0 ->
            FunState2 = case Running of
                            true ->
                                execute(FunState, Wrapper);
                            false ->
                                State#worker_state.funstate
                        end,
            worker_loop(State#worker_state{funstate = FunState2})
    end.

worker_config(State, funstate, FunState) ->
    State#worker_state{funstate=FunState};
worker_config(State, _Cmd, _Args) ->
    State.

execute(#funstate{body=Body, inputs=Ins, state=State}, Wrapper) ->
    InData = get_inputs(Ins),
    {Results, NewFunState} = Body(InData, State),
    send_results(Results, Wrapper),
    NewFunState.

%% use this in your pipe function to get input data
fetch_input(InputId) ->
    receive
        {InputId, Data} ->
            Data
    end.

get_inputs(Ins) when is_list(Ins) ->
    lists:map(fun get_input/1, Ins).

get_input(In) when is_atom(In) ->
    {In, fetch_input(In)};
get_input({In, N}) ->
    {In, get_input(In, N, [])}.

get_input(_In, 0, Res) ->
    lists:reverse(Res);
get_input(In, N, Res) ->
    get_input(In, N-1, [fetch_input(In)|Res]).

send_results([], _) ->
    ok;
send_results(Results, Wrapper) ->
    Wrapper ! {results, Results},
    ok.

%%%%%%%%%%%%%%%%
%%% wrapper %%%

-record(input, {
                pid,
                name,
                count = 0,
                running = false
               }).

-record(output, {
                 pid,
                 name
                }).

-record(wrapper_state, {
                        inputs = [],
                        stop_limit = 100,
                        start_limit = 1,
                        worker,
                        
                        outputs = [],
                        started = 0
                       }).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

wrapper(Opts) ->
    Worker = spawn_link(fun() -> worker(self(), Opts) end),
    wrapper_loop(wrapper_state(Worker, Opts)).

wrapper_state(Worker, Opts) ->
    Inputs = fill(Opts, inputs, #input{}),
    Outputs = fill(Opts, outputs, #output{}),
    #wrapper_state{
                   worker = Worker,
                   inputs = Inputs,
                   outputs = Outputs
                  }.

fill(Opts, Id, Rec) ->
    IL = case pipes_util:get_opt(Id, Opts) of
             {value, L} ->
                 L;
             false ->
                 [default]
         end,
    L0 = lists:duplicate(length(IL), Rec),
    L1 = [setelement(2, R, N) || {R, N} <- lists:zip(L0, IL)],
    L1.


wrapper_loop(#wrapper_state{
                            inputs = Buffers,
                            start_limit = Start,
                            stop_limit = Stop,
                            worker = Worker,
                            started = Started,
                            outputs = Outputs
                           } = State) ->
    receive
        {config, Cmd, Args} ->
            State1 = wrapper_config(State, Cmd, Args),
            wrapper_loop(State1);
        
        %% input
        {data, From, Data} ->
            {value, B, BRest} = lists:keytake(From, #input.pid, Buffers),
            B1 = B#input{count = B#input.count+1},
            worker ! {data, B#input.name, Data},
            B2 = case (B#input.count >=  Stop) and (B#input.running ==  true) of
                     true ->
                         B#input.pid ! stop,
                         B1#input{running = false};
                     false ->
                         B1
                 end,
            wrapper_loop(State#wrapper_state{inputs = [B2|BRest]});
        {fetch, Input} ->
            {value, B, BRest} = lists:keytake(Input, #input.name, Buffers),
            B1 = B#input{count = B#input.count-1},
            B2 = case (B#input.count  =< Start) and (B#input.running ==  false) of
                     true ->
                         B#input.pid ! start,
                         B1#input{running = true};
                     false ->
                         B1
                 end,
            wrapper_loop(State#wrapper_state{inputs = [B2|BRest]});
        
        %%output
        stop ->
            case length(Outputs) of
                Started ->
                    Worker ! stop
            end,
            wrapper_loop(State#wrapper_state{started = Started-1});
        start ->
            case length(Outputs)-1 of
                Started ->
                    Worker ! start
            end,
            wrapper_loop(State#wrapper_state{started = Started+1});
        {result, Results} ->
            Fun = fun({N, RL}) ->
                          {value, Out} = lists:keysearch(N, #output.name, Outputs),
                          [Out ! R || R<-RL],
                          ok
                  end,
            lists:foreach(Fun, Results),
            wrapper_loop(State)
    end.

wrapper_config(State, _Cmd, _Args) ->
    io:format(" *** ~p ~p~n", [_Cmd, _Args]),
    State.


