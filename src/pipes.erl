%% Distributed under the simplified BSD license. See enclosed LICENSE.TXT

-module(pipes).

-export([
         pipe/1,
         pipe/2,
         connect/1,
         connect/2,
         weld/2,
         
         get_result/1
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


