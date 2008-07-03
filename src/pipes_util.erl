%% Distributed under the simplified BSD license. See enclosed LICENSE.TXT 

-module(pipes_util).

-compile([export_all]).

get_opt(Name, Opts) ->
    {value, Val} = lists:keysearch(Name, 1, Opts),
    Val.

