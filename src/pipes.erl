%% Distributed under the simplified BSD license. See enclosed LICENSE.TXT 

-module(pipes).

-export([
         new/1,
         connect/2,
         wrap_simple_fun/1,
         wait_result/1
        ]).

new(Fun) when is_function(Fun) ->
    pipe:start([{body, wrap_simple_fun(Fun)}|default_opts()]);
new(Opts) when is_list(Opts)  ->
    pipe:start(Opts++default_opts()).

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

wait_result(Pipe) ->
    Pipe ! {config, output, {default, {self(), default}}},
    receive 
        {data, default, Data} ->
            Data
    end.


            