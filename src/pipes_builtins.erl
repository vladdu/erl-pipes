-module(pipes_builtins).

-compile([export_all]).

-include("pipes.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% do nothing
null() ->
    #funstate{body=fun null_fun/2}.

null_fun([{stdin, end_data}], _State) ->
    finished;
null_fun(_Ins, _State) ->
    {[], #funstate{body=fun null_fun/2}}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% pass on whatever is input
identity() ->
    #funstate{body=fun identity_fun/2}.

identity_fun([{stdin, In}], _State) ->
    {[{stdout, [In]}], pipes_util:finished(In, #funstate{body=fun identity_fun/2})}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% print to the console
print(Fmt) ->
    #funstate{body=fun print_fun/2, state=Fmt}.

print() ->
    print("~p~n").

print_fun([{stdin, X}], State) ->
    io:format(State, [X]),
    {[], pipes_util:finished(X, #funstate{body=fun print_fun/2, state=State})}.


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
    #funstate{body=fun list_in_fun/2, state=L}.

list_in_fun(_Ins, []) ->
    {[{stdout, [end_data]}], finished};
list_in_fun(_Ins, [H|T]) ->
    {[{stdout, [{data, H}]}], #funstate{body=fun list_in_fun/2, state=T}}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gathers all results into a list
list_out() ->
    #funstate{body=fun list_out_fun/2, state=[]}.

list_out_fun([{stdin, end_data}], List) ->
    {[{stdout, [{data, lists:reverse(List)}, end_data]}], finished};
list_out_fun([{stdin, In}], List) ->
    {[], #funstate{body=fun list_out_fun/2, state=[In|List]}}.

