-module(pipes_test).

-compile([export_all]).

test() ->
    N0 = length(processes()),
    
    spawn(fun test1/0),
    spawn(fun test2/0),
    spawn(fun test3/0),
    spawn(fun test4/0),
    spawn(fun test5/0),
    spawn(fun test6/0),
    
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
    pipes:connect({P2, nomatch}, pipes:pipe(null)),
    
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

test6() ->
    P1 = pipes:pipe(from_list, [lists:seq(1,20)]),
    P3 = pipes:pipe(partition, [fun(X) -> X rem 4 == 0 end]),
    P4 = pipes:pipe(to_list),
    P5 = pipes:pipe(identity),
    P6 = pipes:pipe(map, [fun(X) -> X div 2 end]),
    P7 = pipes:pipe(to_list),
    
    pipes:connect([P1, P3, P4]),
    pipes:connect([{P3, nomatch}, P5, P6, P7]),
    
    R = pipes:get_result(P4),
    io:format("6**** Result: match ~p: ~p~n", [length(R), R]),
    R1 = pipes:get_result(P7),
    io:format("6**** Result: nomatch ~p: ~p~n", [length(R1), R1]),
    ok.


-define(F2, "c:/downloads/mcreate.xml").
-define(F1, "c:/downloads/n.xml").

bmark() ->
    bmark("Short", ?F1),
    bmark("Long", ?F2).


bmark(_Name, N) ->
    Me = self(),
    spawn(fun()->
                  T1 = timer:tc(pipes_test, bmark_old, [N]),
                  Me ! T1
          end),
    receive {T1, V1} ->  
                io:format("~s Old style: ~p~n", [N, {T1, V1}])
    end,
    spawn(fun() ->
                  T2 = timer:tc(pipes_test, bmark_new, [N]),
                  Me ! T2
          end),
    receive {T2, V2} ->  
                io:format("~s New style: ~p~n", [N, {T2, V2}])
    end,
    io:format("d=~p, f=~p~n", [T2-T1, T2/T1]),
    ok.


bmark_old(FN) ->
    {ok, Bin} = file:read_file(FN++"1"),
    L = binary_tokens(Bin, <<"\r\n">>),
    Fun = fun(X)->
                  L_ = binary_to_list(X), 
                  regexp:first_match(L_, "=") =/= nomatch
          end,
    {L1, L2} = lists:partition(Fun, L),
    {length(L1), length(L2)}.

binary_tokens(Bin, Term) ->
    binary_tokens(Bin, Term, []).

binary_tokens(Bin, Term, Res) ->
    case bfind(Bin, Term, 0) of
        not_found ->
            lists:reverse([Bin|Res]);
        N ->
            {H, T} = split_binary(Bin, N),
            {_, T1} = split_binary(T, size(Term)),
            binary_tokens(T1, Term, [H|Res])
    end.

bfind(<<>>, _Term, _N) ->
    not_found;
bfind(Bin, Term, N) ->
    Len = size(Term),
    case Bin of
        <<Term:Len/binary, _/binary>> ->
            N;
        <<_:8, Rest/binary>> ->
            bfind(Rest, Term, N+1)
    end.


bmark_new(FN) ->
    P1 = pipes:pipe(cat, [FN++"2", 8192]),
    P2 = pipes:pipe(lines),
    P4 = pipes:pipe(grep, ["="]),
    P3 = pipes:pipe(count),
    P5 = pipes:pipe(count),
    
    pipes:connect([P1, P2, P4, P3]),
    pipes:connect({P4, nomatch}, P5),
    
    {pipes:get_result(P3), pipes:get_result(P5)}.

