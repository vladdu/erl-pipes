%% Distributed under the simplified BSD license. See enclosed LICENSE.TXT

-module(pipes).

-export([
         new/1,
         new/2,
         connect/2,
         wrap_simple_fun/1,
         get_result/1,

         test/0
        ]).

new(Fun) when is_function(Fun) ->
    pipe:start_link([{body, wrap_simple_fun(Fun)}|default_opts()]);
new(Opts) when is_list(Opts)  ->
    pipe:start_link(Opts++default_opts()).

new(Fun, Opts) ->
    new([{body, wrap_simple_fun(Fun)}|[Opts|default_opts()]]).

connect({Pipe1, Output1}, {Pipe2, Input2}) ->
    Pipe1 ! {config, output, {Output1, {Pipe2, Input2}}},
    Pipe2 ! {config, input, {Input2, {Pipe1, Output1}}},
    ok;
connect(Pipe1, Pipe2) ->
    connect({Pipe1, default}, {Pipe2, default}).

wrap_simple_fun(Fun) ->
    Fun.

default_opts() ->
    [].

get_result(Pipe) ->
    Pipe ! {config, output, {default, {self(), default}}},
    receive
        {data, default, Data} ->
            Data
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%5

test() ->
    dbg:stop(),
    dbg:tracer(),
    spawn(fun test1/0).

test1() ->
    dbg:tp(pipes, []),
    dbg:tp(pipe, []),
    dbg:tp(pipes_builtins, []),
    dbg:tp(pipes_util, []),
    dbg:p(self(), [m,c,sos]),

    P1 = new(pipes_builtins:list_in([1, 2, 3]), [{inputs, []}]),
    P2 = new(pipes_builtins:list_out()),
    connect(P1, P2),
    %R = get_result(P2),
    %io:format("Result: ~p~n", [R]),
    {P1, P2}.
