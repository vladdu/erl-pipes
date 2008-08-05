%% Distributed under the simplified BSD license. See enclosed LICENSE.TXT

-module(pipes_util).

-compile([export_all]).

get_opt(Name, Opts) ->
    lists:keysearch(Name, 1, Opts).

finished(end_data, State) ->
    finished;
finished(_, State) ->
    State.
