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

-record(worker_state, {
                       wrapper,
                       body = fun identity/2,
                       running = false
                      }).

identity(In, S) ->
    {In, S, fun identity/2}.

worker(Wrapper, Opts) ->
    worker_loop(worker_state(Wrapper, Opts)).

worker_state(Wrapper, Opts) ->
    #worker_state{wrapper = Wrapper}.

worker_loop(#worker_state{body = _Body,
                          running = Running} = State)->
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
            worker_loop(State2#worker_state{running = false});
        start ->
            worker_loop(State2#worker_state{running = true})
    end.

worker_config(State, _Cmd, _Args) ->
    State.

execute(State) ->
    %% fetch all required inputs
    %% do the processing
    %% send results
    
    %% the state should be only what is required for the processing, not everything
    
    State.

fetch_input(InputId) ->
    receive 
        {data, InputId, Data} -> 
            {data, Data}; 
        {'$end', InputId} -> 
            '$end' 
    end.


%%%%%%%%%%%%%%%%

-record(input, {
                pid,
                index,
                count = 0,
                running = false
               }).

-record(output, {
                 pid, 
                 index
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
    BL = pipes_util:get_opt(buffers, Opts),
    Buffers = lists:duplicate(lists:length(BL), #input{}),
    Buffers1 = [R#input{index = I} || {R, I} <- lists:zip(Buffers, lists:seq(lists:length(Buffers)))],
    #wrapper_state{
             worker = Worker,
             inputs = Buffers1
            }.

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
            worker ! {data, B#input.index, Data},
            B2 = case (B#input.count >=  Stop) and (B#input.running ==  true) of
                     true ->
                         B#input.pid ! stop,
                           B1#input{running = false};
                     false ->
                         B1
                 end,
            wrapper_loop(State#wrapper_state{inputs = [B2|BRest]});
        {fetch, Input} ->
            {value, B, BRest} = lists:keytake(Input, #input.index, Buffers),
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
                            {value, Out} = lists:keysearch(N, #output.index, Outputs),
                          [Out ! R || R<-RL],
                          ok
                  end,
            lists:foreach(Fun, Results),
            wrapper_loop(State)
    end.

wrapper_config(State, _Cmd, _Args) ->
    State.


