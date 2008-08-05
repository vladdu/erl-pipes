%% Distributed under the simplified BSD license. See enclosed LICENSE.TXT

-module(pipes_builtins).

-compile([export_all]).

-include("pipes.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% do nothing
null() ->
    #context{body=fun null_fun/2}.

null_fun([{default, end_data}], _State) ->
    finished;
null_fun(_Ins, _State) ->
    {[], #context{body=fun null_fun/2}}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% pass on whatever is input
identity() ->
    #pipe{context = #context{body=fun identity_fun/2}}.

identity_fun([{default, end_data}], _State) ->
    {[{default, [end_data]}], #context{finished=true}};
identity_fun([{default, In}], _State) ->
    {[{default, [In]}], #context{body=fun identity_fun/2}}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% print to the console
print(Fmt) ->
    #context{body=fun print_fun/2, state=Fmt}.

print() ->
    print("~p~n").

print_fun([{default, X}], State) ->
    io:format(State, [X]),
    {[], pipes_util:finished(X, #context{body=fun print_fun/2, state=State})}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% read binary stream from a file
%% binfile_in(FileName, Options) ->
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
%% erl_file_in(FileName) ->
%%     {ok, Terms} = file:consult(FileName),
%%     list_in(Terms).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% get input from a list
list_in(L) when is_list(L) ->
    #pipe{inputs = [], 
          context = #context{body=fun list_in_fun/2, state=L, inputs=[]}}.

list_in_fun(_Ins, []) ->
    {[{default, [end_data]}], #context{finished=true}};
list_in_fun(_Ins, [H|T]) ->
    {[{default, [{data, self(), H}]}], #context{body=fun list_in_fun/2, state=T, inputs=[]}}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gathers all results into a list
list_out() ->
    #pipe{context = #context{body=fun list_out_fun/2, state=[]}}.

list_out_fun([{default, end_data}], List) ->
    {[{default, [{data, lists:reverse(List)}, end_data]}], finished};
list_out_fun([{default, In}], List) ->
    {[], #context{body=fun list_out_fun/2, state=[In|List]}}.

