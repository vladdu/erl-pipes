%% Distributed under the simplified BSD license. See enclosed LICENSE.TXT

-module(pipes).

-export([
         new/1,
         connect/1,
         connect/2,
         weld/2,
         
         get_result/1,
         
         test/0
        ]).

-include("pipes.hrl").

new(Pipe) when is_record(Pipe, pipe) ->
    pipe:start_link(Pipe).

connect({Pid1, Name1}, {Pid2, Name2}) ->
    Pid1 ! {output, Name1, Pid2},
    Pid2 ! {input, Name2, Pid1},
    ok;
connect({Pid1, Name1}, Pid2) ->
    connect({Pid1, Name1}, {Pid2, default});
connect(Pid1, {Pid2, Name2}) ->
    connect({Pid1, default}, {Pid2, Name2});
connect(Pid1, Pid2) ->
    connect({Pid1, default}, {Pid2, default}).

connect([H|T]) ->
    connect_trunk(T, H, H).

weld(P1, P2) ->
    connect(P1, P2),
    {P1, P2}.

connect_trunk([], Pid1, Pid2) ->
    {Pid1, Pid2};
connect_trunk([H|T], P1, P2) ->
    connect(P2, H),
    connect_trunk(T, P1, H).

get_result(Pipe) ->
    Pipe ! {output, default, self()},
    Pipe ! start,
    receive
        {data, _, {data, Data}} ->
            Data
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%5

test() ->
    spawn(fun test1/0),
    spawn(fun test2/0).

test1() ->
    P1 = new(pipes_builtins:bcat("../src/pipes_util.erl", 128)),
    %% you might want to use the platform's line endings here
    P4 = new(pipes_builtins:bsplit(<<"\r\n">>)),
    P5 = new(pipes_builtins:fan_out([default, counter])),
    P3 = new(pipes_builtins:to_list()),
    C = new(pipes_builtins:count()),

    connect([P1, P4, P5, P3]),
    connect({P5, counter}, C),
    
    R = get_result(P3),
    io:format("**** Result: ~p: ~p~n", [length(R), R]),
    N = get_result(C),
    io:format("**** Count: ~p~n", [N]),
    ok.

test2() ->
    P1 = new(pipes_builtins:from_list([1,2,3,4,5])),
    P2 = new(pipes_builtins:map(fun(X) -> X+$A end)),
    P3 = new(pipes_builtins:to_list()),
    
    connect([P1, P2, P3]),
    
    R = get_result(P3),
    io:format("**** Result: ~p: ~p~n", [length(R), R]),
    ok.
