%% Distributed under the simplified BSD license. See enclosed LICENSE.TXT

-module(pipes_builtins).

-compile([export_all]).

-include("pipes.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% do nothing
null() ->
    #context{body=fun null_fun/2}.

null_fun([{default, end_data}], _State) ->
    #context{finished=true};
null_fun(_Ins, _State) ->
    {[], #context{body=fun null_fun/2}}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% pass on whatever is input
identity() ->
    #pipe{name = identity, 
          context = #context{body=fun identity_fun/2}}.

identity_fun([{default, end_data}], _State) ->
    {[{default, [end_data]}], #context{finished=true}};
identity_fun([{default, In}], _State) ->
    {[{default, [In]}], #context{body=fun identity_fun/2}}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% print to the console
print(Fmt) ->
    #pipe{name = print,
          context = #context{body=fun print_fun/2, state=Fmt}}.

print() ->
    print("~p~n").

print_fun([{default, X}], State) ->
    io:format(State, [X]),
    {[], pipes_util:finished(X, #context{body=fun print_fun/2, state=State})}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% read binary stream from a file
%% bcat(FileName, Options) ->
%%     fun(_X, init) ->
%%             {ok, File} = file:open(FileName, [read_ahead, raw, binary | Options]),
%%             {[], File};
%%        (_X, File) ->
%%             case file:read(File, 1024) of
%%                 {ok, Chunk} ->
%%                     {[Chunk], File};
%%                 eof ->
%%                     file:close(File),
%%                     {['$end'], ok}
%%             end
%%     end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% read Erlang terms from a file
%% ecat(FileName) ->
%%     {ok, Terms} = file:consult(FileName),
%%     from_list(Terms).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% get input from a list
from_list(L) when is_list(L) ->
    #pipe{name = from_list,
          inputs = [], 
          context = #context{body=fun from_list_fun/2, state=L, inputs=[]}}.

from_list_fun(_Ins, []) ->
    {[{default, [end_data]}], #context{finished=true}};
from_list_fun(_Ins, [H|T]) ->
    {[{default, [{data, H}]}], #context{body=fun from_list_fun/2, state=T, inputs=[]}}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gathers all results into a list
to_list() ->
    #pipe{name = to_list,
          context = #context{body=fun to_list_fun/2, state=[]}}.

to_list_fun([{default, end_data}], List) ->
    {[{default, [{data, lists:reverse(List)}, end_data]}], #context{finished=true}};
to_list_fun([{default, {data, In}}], List) ->
    {[], #context{body=fun to_list_fun/2, state=[In|List]}}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% fan out
fan_out(Outs) ->
    #pipe{name = fan_out,
          outputs = Outs,
          context = #context{body=fun fan_out_fun/2, state=Outs}}.

fan_out_fun([{default, In}], Outs) ->
    Out = [{X, [In]} || X <- Outs],
    Fin = (In==end_data),
    {Out, #context{body=fun fan_out_fun/2, finished=Fin, state=Outs}}.

  
  
  