%% Distributed under the simplified BSD license. See enclosed LICENSE.TXT 

-module(pipes).

-export([
         new/1,
         connect/2,
         wrap_simple_fun/1
        ]).

new(Opts) ->
    pipe:start(Opts).

connect({Pipe1, Output1}, {Pipe2, Input2}) ->
    Pipe1 ! {config, output, {Output1, {Pipe2, Input2}}},
    Pipe2 ! {config, input, {Input2, {Pipe1, Output1}}},
    ok;
connect(Pipe1, Pipe2) ->
    connect({Pipe1, default}, {Pipe2, default}).

wrap_simple_fun(Fun) ->
    Fun.

