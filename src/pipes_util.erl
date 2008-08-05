%% Distributed under the simplified BSD license. See enclosed LICENSE.TXT

-module(pipes_util).

-compile([export_all]).

-include("pipes.hrl").

get_opt(Name, Opts) ->
    lists:keysearch(Name, 1, Opts).

finished(end_data, State) ->
    State#context{finished=true};
finished(_, State) ->
    State.
