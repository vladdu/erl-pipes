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

-include("pipes.hrl").

new(Pipe) when is_record(Pipe, pipe) ->
    pipe:start_link(Pipe).

new(Fun, Opts) ->
    new([{body, wrap_simple_fun(Fun)}|[Opts|default_opts()]]).

connect({Name1, Pid1}, {Name2, Pid2}) ->
    Pid1 ! {output, Name2, Pid2},
    Pid2 ! {input, Name1, Pid1},
    ok;
connect(Pipe1, Pipe2) ->
    connect({default, Pipe1}, {default, Pipe2}).

wrap_simple_fun(Fun) ->
    Fun.

default_opts() ->
    [].

get_result(Pipe) ->
    Pipe ! {output, default, self()},
    receive
        Data -> %% {data, default, Data} ->
            Data
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%5

test() ->
    dbg:stop(),
    dbg:start(),
    %%Log="log.log",
    %%Fun2 = dbg:trace_port(file, Log),
    %%{ok, TPid} = dbg:tracer(port, Fun2),
    dbg:tracer(),
    spawn(fun test1/0).

test1() ->
    dbg:tp(pipes, []),
    dbg:tp(pipe, []),
    dbg:tp(pipes_builtins, []),
    dbg:tp(pipes_util, []),
    %%     dbg:tpl(pipes, []),
    %%     dbg:tpl(pipe, []),
    %%     dbg:tpl(pipes_builtins, []),
    %%     dbg:tpl(pipes_util, []),
    dbg:p(self(), [p, c, sos]),
    
    P1 = new(pipes_builtins:list_in([1, 2, 3])),
    P = new(pipes_builtins:identity()), 
    P2 = new(pipes_builtins:list_out()),
    
    io:format("~p~n", [{P1, P, P2}]),
    
    connect(P1, P),
    connect(P, P2),
    
    dbg:stop(),
    
    R = get_result(P2),
    io:format("**** Result: ~p~n", [R]),
    R.