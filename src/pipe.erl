-module(pipe).

-export([new/1]).

%-compile([export_all]).

new(Opts) ->
    Input = spawn(fun() -> input(Opts) end),
    Output = spawn(fun() -> output(Opts) end),
    _Worker = spawn(fun() -> worker(Input, Output, Opts) end),
    {Input, Output}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(worker, {
                 input,
                 output,
                 body=fun identity/2,
                 running=false
                }).

identity(In, S) ->
    {In, S, fun identity/2}.

worker(Input, Output, Opts) ->
    Input ! {config, worker, self()},
    worker_loop(worker_state(Input, Output, Opts)).

worker_state(Input, Output, _Opts) ->
    #worker{input=Input, output=Output}.

worker_loop(#worker{body=_Body,
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
    %% fetch all required inputs
    %% do the processing
    %% send results

    %% the state should be only what is required for the processing, not everything

    State.

%%%%%%%%%%%%%%%%

-record(input, {
                buffer_data = [],
                stop_limit = 100,
                start_limit = 1,
                worker
               }).
-record(input_data, {
                     pid,
                     index,
                     count=0,
                     running=false
                    }).

input(Opts) ->
    input_loop(input_state(Opts)).

get_opt(Name, Opts) ->
    {value, Val} = lists:keysearch(Name, 1, Opts),
    Val.

input_state(Opts) ->
    BL = get_opt(buffers, Opts),
    Buffers = lists:duplicate(lists:length(BL), #input_data{}),
    Buffers1 = [R#input_data{index=I} || {R, I} <- lists:zip(Buffers, lists:seq(lists:length(Buffers)))],
    #input{
           buffer_data = Buffers1
          }.

input_loop(#input{
                  buffer_data = Buffers,
                  start_limit=Start,
                  stop_limit=Stop,
                  worker=worker
                 }=State) ->
    receive
        {config, Cmd, Args} ->
            State1 = input_config(State, Cmd, Args),
            input_loop(State1);
        {data, From, Data} ->
            {value, B, BRest} = lists:keytake(From, #input_data.pid, Buffers),
            B1 = B#input_data{count=B#input_data.count+1},
            worker ! {data, B#input_data.index, Data},
            B2 = case (B#input_data.count >= Stop) and (B#input_data.running == true) of
                     true ->
                         B#input_data.pid ! stop,
                           B1#input_data{running=false};
                     false ->
                         B1
                 end,
            input_loop(State#input{buffer_data=[B2|BRest]});
        {fetch, Input} ->
            {value, B, BRest} = lists:keytake(Input, #input_data.index, Buffers),
            B1 = B#input_data{count=B#input_data.count-1},
            B2 = case (B#input_data.count =< Start) and (B#input_data.running == false) of
                     true ->
                         B#input_data.pid ! start,
                           B1#input_data{running=true};
                     false ->
                         B1
                 end,
            input_loop(State#input{buffer_data=[B2|BRest]})
    end.

input_config(State, _Cmd, _Args) ->
    State.


%%%%%%%%%%%%%%%%

-record(output, {
                 outputs=[],
                 worker,
                 started=0
                }).
-record(outrec, {pid, index}).

output(Opts) ->
    output_loop(output_state(Opts)).

output_state(_Opts) ->
    #output{
            }.

output_loop(#output{
                    worker=Worker,
                    started=Started,
                    outputs=Outputs
                   }=State) ->
    receive
        {config, Cmd, Args} ->
            State1 = output_config(State, Cmd, Args),
            output_loop(State1);
        stop ->
            case length(Outputs) of
                Started ->
                    Worker ! stop
            end,
            output_loop(State#output{started=Started-1});
        start ->
            case length(Outputs)-1 of
                Started ->
                    Worker ! start
            end,
            output_loop(State#output{started=Started+1});
        {result, Results} ->
            Fun = fun({N, RL}) ->
                          {value, Out} = lists:keysearch(N, #outrec.index, Outputs),
                          [Out ! R || R<-RL],
                          ok
                  end,
            lists:foreach(Fun, Results),
            output_loop(State)
    end.

output_config(State, _Cmd, _Args) ->
    State.



