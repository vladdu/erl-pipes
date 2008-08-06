%% Distributed under the simplified BSD license. See enclosed LICENSE.TXT

-module(pipes).

-export([
         pipe/1,
         pipe/2,
         connect/1,
         connect/2,
         weld/2,
         
         get_result/1,
         
         test/0
        ]).

-include("pipes.hrl").

pipe(Pipe) when is_record(Pipe, pipe) ->
    pipe:start_link(Pipe);
pipe(Pipe) when is_atom(Pipe) ->
    pipe(Pipe, []).

pipe(Pipe, Args) ->
    pipe(pipes_builtins, Pipe, Args).

pipe(Module, Pipe, Args) ->
    pipe:start_link(erlang:apply(Module, Pipe, Args)).

connect({Pid1, Name1}, {Pid2, Name2}) ->
    Pid1 ! {output, Name1, Pid2},
    Pid2 ! {input, Name2, Pid1},
    ok;
connect({Pid1, Name1}, Pid2) ->
    connect({Pid1, Name1}, {Pid2, stdin});
connect(Pid1, {Pid2, Name2}) ->
    connect({Pid1, stdout}, {Pid2, Name2});
connect(Pid1, Pid2) ->
    connect({Pid1, stdout}, {Pid2, stdin}).

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
    Pipe ! {output, stdout, self()},
    Pipe ! start,
    receive
        {data, _, {data, Data}} ->
            Data
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%5

test() ->
    N0 = length(processes()),
    
    spawn(fun test1/0),
    spawn(fun test2/0),
    spawn(fun test3/0),
    spawn(fun test4/0),
    spawn(fun test5/0),
    
    receive after 1000 -> ok end,
    N0 = length(processes()),
    ok.

test1() ->
    P1 = pipe(bcat, ["../src/pipes_util.erl", 128]),
    P4 = pipe(blines),
    P5 = pipe(fan_out, [[stdout, counter]]),
    P3 = pipe(to_list),
    C = pipe(count),
    
    connect([P1, P4, P5, P3]),
    connect({P5, counter}, C),
    
    R = get_result(P3),
    io:format("1**** Result: ~p: ~p~n", [length(R), R]),
    N = get_result(C),
    io:format("1**** Count: ~p~n", [N]),
    ok.

test2() ->
    P1 = pipe(from_list, [[1,2,3,4,5]]),
    P2 = pipe(map, [fun(X) -> X+$A end]),
    P4 = pipe(filter, [fun(X) -> (X rem 2) == 0 end]),
    P3 = pipe(to_list),
    
    connect([P1, P2, P4, P3]),
    
    R = get_result(P3),
    io:format("2**** Result: ~p: ~p~n", [length(R), R]),
    ok.

test3() ->
    P1 = pipe(cat, ["../src/pipes_util.erl", 128]),
    %% you might want to use the platform's line endings here
    P4 = pipe(lines),
    P2 = pipe(grep, ["->"]),
    P3 = pipe(to_list),
    
    connect([P1, P4, P2, P3]),
    
    R = get_result(P3),
    io:format("3**** Result: ~p: ~p~n", [length(R), R]),
    ok.

test4() ->
    P1 = pipe(cat, ["../src/pipes_util.erl", 128]),
    %% you might want to use the platform's line endings here
    P4 = pipe(splitx, ["\r?\n"]),
    P3 = pipe(to_list),
    
    connect([P1, P4, P3]),
    
    R = get_result(P3),
    io:format("4**** Result: ~p: ~p~n", [length(R), R]),
    ok.

test5() ->
    P1 = pipe(from_list, [[1,2,3,4,5]]),
    P2 = pipe(from_list, [[11,12,13,14,15]]),
    P3 = pipe(zip),
    P4 = pipe(to_list),
    
    connect(P1, {P3, in2}),
    connect(P2, {P3, in1}),
    connect(P3, P4),
    
    R = get_result(P4),
    io:format("5**** Result: ~p: ~p~n", [length(R), R]),
    ok.

