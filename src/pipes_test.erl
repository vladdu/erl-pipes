-module(pipes_test).

-compile([export_all]).

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
    P1 = pipes:pipe(bcat, ["../src/pipes_util.erl", 128]),
    P4 = pipes:pipe(blines),
    P5 = pipes:pipe(fan_out, [[stdout, counter]]),
    P3 = pipes:pipe(to_list),
    C = pipes:pipe(count),
    
    pipes:connect([P1, P4, P5, P3]),
    pipes:connect({P5, counter}, C),
    
    R = pipes:get_result(P3),
    io:format("1**** Result: ~p: ~p~n", [length(R), R]),
    N = pipes:get_result(C),
    io:format("1**** Count: ~p~n", [N]),
    ok.

test2() ->
    P1 = pipes:pipe(from_list, [[1,2,3,4,5]]),
    P2 = pipes:pipe(map, [fun(X) -> X+$A end]),
    P4 = pipes:pipe(filter, [fun(X) -> (X rem 2) == 0 end]),
    P3 = pipes:pipe(to_list),
    
    pipes:connect([P1, P2, P4, P3]),
    
    R = pipes:get_result(P3),
    io:format("2**** Result: ~p: ~p~n", [length(R), R]),
    ok.

test3() ->
    P1 = pipes:pipe(cat, ["../src/pipes_util.erl", 128]),
    P4 = pipes:pipe(lines),
    P2 = pipes:pipe(grep, ["->"]),
    P3 = pipes:pipe(to_list),
    
    pipes:connect([P1, P4, P2, P3]),
    
    R = pipes:get_result(P3),
    io:format("3**** Result: ~p: ~p~n", [length(R), R]),
    ok.

test4() ->
    P1 = pipes:pipe(cat, ["../src/pipes_util.erl", 128]),
    P4 = pipes:pipe(splitx, ["\r?\n"]),
    P3 = pipes:pipe(to_list),
    
    pipes:connect([P1, P4, P3]),
    
    R = pipes:get_result(P3),
    io:format("4**** Result: ~p: ~p~n", [length(R), R]),
    ok.

test5() ->
    P1 = pipes:pipe(from_list, [[1,2,3,4,5]]),
    P2 = pipes:pipe(from_list, [[11,12,13,14,15]]),
    P3 = pipes:pipe(zip),
    P4 = pipes:pipe(to_list),
    
    pipes:connect(P1, {P3, in2}),
    pipes:connect(P2, {P3, in1}),
    pipes:connect(P3, P4),
    
    R = pipes:get_result(P4),
    io:format("5**** Result: ~p: ~p~n", [length(R), R]),
    ok.

