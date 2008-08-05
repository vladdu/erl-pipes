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

wrap_simple_fun(Fun) ->
    Fun.

default_opts() ->
    [].

get_result(Pipe) ->
    Pipe ! {output, default, self()},
    Pipe ! start,
    receive
        {data, _, {data, Data}} ->
            Data
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%5

test() ->
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
    %% dbg:tpl(pipes, []),
    % dbg:tpl(pipe, []),
    %% dbg:tpl(pipes_builtins, []),
    %% dbg:tpl(pipes_util, []),
    dbg:p(self(), [m, c, p, sos]),
    
    P1 = new(pipes_builtins:from_list([1, 2, 3])),
    P2 = new(pipes_builtins:identity()), 
    P3 = new(pipes_builtins:to_list()),
    P4 = new(pipes_builtins:print()),
    P5 = new(pipes_builtins:fan_out([o1, o2])),
    
    connect(P1, P2),
    connect(P2, P5),
    connect({P5, o1}, P3),
    connect({P5, o2}, P4),
   
    R = get_result(P3),
    io:format("**** Result: ~p~n", [R]),
    R.