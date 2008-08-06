%% Distributed under the simplified BSD license. See enclosed LICENSE.TXT

-module(pipes_util).

-compile([export_all]).

-include("pipes.hrl").

get_opt(Name, Opts) ->
    lists:keysearch(Name, 1, Opts).

wrap(Fun) ->
    fun([{stdin, D}], #context{state=SS}=S0) ->
            {R, S, Fin} = Fun(D, SS),
            R1 = [{stdout, R}],
            {R1, S0#context{state=S, finished=Fin}};
       (_, #context{state=SS}=S0) ->
            {R, S, Fin} = Fun(ok, SS),
            R1 = [{stdout, [X]} || X<-R],
            {R1, S0#context{state=S, finished=Fin}}
    end.

