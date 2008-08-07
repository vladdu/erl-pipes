%% Distributed under the simplified BSD license. See enclosed LICENSE.TXT

-module(pipes_builtins).

-export([
         null/0,
         identity/0,
         fan_out/1,
         
         map/1,
         flatmap/1,
         fold/2,
         filter/1,
         zipwith/1,
         zipwith/2,
         zip/0,
         zip/1,
         partition/1,
         
%%          from_list/0,
         from_list/1,
         to_list/0,
         
         print/0,
         print/1,
         
         bcat/1,
         bcat/2,
         cat/1,
         cat/2,
         ecat/1,
         bsplit/1,
         blines/0,
         bwords/0,
         split/1,
         lines/0,
         words/0,
         splitx/1,
         count/0,
         grep/1
        ]).

-include("pipes.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% do nothing
null() ->
    #pipe{name = null,
          outputs = [],
          context = #context{body = pipes_util:wrap(fun null_fun/2)}}.

null_fun(end_data, S) ->
    {[], S, true};
null_fun({data, _X}, S) ->
    {[], S, false}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% pass on whatever is input
identity() ->
    #pipe{name = identity, 
          context = #context{body = pipes_util:wrap(fun identity_fun/2)}}.

identity_fun(end_data, State) ->
    {[end_data], State, true};
identity_fun(In, State) ->
    {[In], State, false}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% process each element
map(Fun) ->
    #pipe{name = map, 
          context = #context{body = pipes_util:wrap(fun map_fun/2), 
                             state = Fun}}.

map_fun(end_data, State) ->
    {[end_data], State, true};
map_fun({data, In}, Fun) ->
    {[{data, Fun(In)}], Fun, false}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% process each element
flatmap(Fun) ->
    #pipe{name = map, 
          context = #context{body = pipes_util:wrap(fun flatmap_fun/2), 
                             state = Fun}}.

flatmap_fun(end_data, State) ->
    {[end_data], State, true};
flatmap_fun({data, In}, Fun) ->
    case Fun(In) of
        [] ->
            {[], Fun, false};
        R ->
            {[{data, X} || X <- R], Fun, false}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% fold
fold(Fun, Acc) ->
    #pipe{name = fold,
          context = #context{body = pipes_util:wrap(fun fold_fun/2), 
                             state = {Fun, Acc}}}.

fold_fun(end_data, {_, Acc}=State) ->
    {[{data, Acc}, end_data], State, true};
fold_fun({data, In}, {Fun, Acc}) ->
    {[], {Fun, Fun(In, Acc)}, false}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% filter
filter(Fun) ->
    #pipe{name = filter, 
          context = #context{body = pipes_util:wrap(fun filter_fun/2), 
                             state = Fun}}.

filter_fun(end_data, State) ->
    {[end_data], State, true};
filter_fun({data, In}, Fun) ->
    case Fun(In) of
        true ->
            {[{data, In}], Fun, false};
        false ->
            {[], Fun, false}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% print to the console
print(Fmt) ->
    #pipe{name = print,
          outputs=[],
          context = #context{body = pipes_util:wrap(fun print_fun/2), 
                             state = Fmt}}.

print() ->
    print("~p~n").

print_fun(X, Fmt) ->
    io:format(Fmt, [X]),
    Fin = (X==end_data),
    {[], Fmt, Fin}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% read binary stream from a file
bcat(FileName) ->
    bcat(FileName, 8192).

bcat(FileName, ChunkSize) ->
    {ok, File} = file:open(FileName, [read_ahead, binary]),
    #pipe{name = bcat,
          inputs = [],
          context = #context{body = pipes_util:wrap(fun bcat_fun/2), 
                             inputs = [], 
                             state = {File, ChunkSize}}}.

bcat_fun(_, {File, ChunkSize}=State) ->
    case file:read(File, ChunkSize) of
        {ok, Chunk} ->
            {[{data, Chunk}], State, false};
        _ ->
            file:close(File),
            {[end_data], State, true}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% read list stream from a file
cat(FileName) ->
    cat(FileName, 8192).

cat(FileName, ChunkSize) ->
    {ok, File} = file:open(FileName, [read_ahead]),
    #pipe{name = cat,
          inputs = [],
          context = #context{body = pipes_util:wrap(fun cat_fun/2), 
                             inputs = [], 
                             state = {File, ChunkSize}}}.

cat_fun(_, {File, ChunkSize}=State) ->
    case file:read(File, ChunkSize) of
        {ok, Chunk} ->
            {[{data, Chunk}], State, false};
        _ ->
            file:close(File),
            {[end_data], State, true}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% split a binary 
bsplit(Term) ->
    #pipe{name = bsplit,
          context = #context{body = pipes_util:wrap(fun bsplit_fun/2), 
                             state = {Term, <<>>}}}.

blines() ->
    %% you might want to use the platform's line endings here
    bsplit(<<"\r\n">>).

bwords() ->
    bsplit(<<" ">>).

bsplit_fun(end_data, {_Term, Rest}=State) ->
    {[{data, Rest}, end_data], State, true};
bsplit_fun({data, In}, {Term, Rest}) ->
    Bin = <<Rest/binary, In/binary>>,
    {Tokens, Rest2} = binary_tokens(Bin, Term),
    Data = [{data, T} || T<-Tokens],
    {Data, {Term, Rest2}, false}.

binary_tokens(Bin, Term) ->
    binary_tokens(Bin, Term, []).

binary_tokens(Bin, Term, Res) ->
    case bfind(Bin, Term, 0) of
        not_found ->
            {lists:reverse(Res), Bin};
        N ->
            {H, T} = split_binary(Bin, N),
            {_, T1} = split_binary(T, size(Term)),
            binary_tokens(T1, Term, [H|Res])
    end.

bfind(<<>>, _Term, _N) ->
    not_found;
bfind(Bin, Term, N) ->
    Len = size(Term),
    case Bin of
        <<Term:Len/binary, _/binary>> ->
            N;
        <<_:8, Rest/binary>> ->
            bfind(Rest, Term, N+1)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% split a list 
split(Term) ->
    #pipe{name = split,
          context = #context{body = pipes_util:wrap(fun split_fun/2), 
                             state = {Term, []}}}.

lines() ->
    %% you might want to use the platform's line endings here
    split("\r\n").

words() ->
    split(" ").

split_fun(end_data, {_Term, Rest}=State) ->
    {[{data, Rest}, end_data], State, true};
split_fun({data, In}, {Term, Rest}) ->
    List = Rest ++ In,
    {Tokens, Rest2} = tokens(List, Term),
    Data = [{data, T} || T<-Tokens],
    {Data, {Term, Rest2}, false}.

tokens(Bin, Term) ->
    tokens(Bin, Term, []).

tokens(List, Term, Res) ->
    case string:str(List, Term) of
        0 ->
            {lists:reverse(Res), List};
        N ->
            {H, T} = lists:split(N-1, List),
            {_, T1} = lists:split(length(Term), T),
            tokens(T1, Term, [H|Res])
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% split a list with regexp 
splitx(Term) ->
    #pipe{name = split,
          context = #context{body = pipes_util:wrap(fun splitx_fun/2), 
                             state = {Term, []}}}.

splitx_fun(end_data, {_Term, Rest}=State) ->
    {[{data, Rest}, end_data], State, true};
splitx_fun({data, In}, {Term, Rest}) ->
    {ok, Tokens} = regexp:split(Rest++In, Term),
    [H | Ts] = lists:reverse(Tokens),
    Data = [{data, T} || T<-lists:reverse(lists:filter(fun(X) -> X=/=[] end, Ts))],
    {Data, {Term, H}, false}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% read Erlang terms from a file
ecat(FileName) ->
    {ok, Terms} = file:consult(FileName),
    from_list(Terms).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% get input from a list
from_list(L) when is_list(L) ->
    #pipe{name = from_list_1,
          inputs = [], 
          context = #context{body = pipes_util:wrap(fun from_list_fun/2), 
                             state = L, 
                             inputs = []}}.

%% from_list() ->
%%     #pipe{name = from_list_0,
%%           context = #context{body = fun from_list0_fun/2, 
%%                              state = []}}.

from_list_fun(_, []) ->
    {[end_data], [], true};
from_list_fun(_, [H|T]) ->
    {[{data, H}], T, false}.

%% from_list0_fun([{stdin, end_data}], S) ->
%%     {[], S};
%% from_list0_fun([{stdin, {data, L}}], S) ->
%%     {[], S#context{state = L}}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gathers all results into a list
to_list() ->
    #pipe{name = to_list,
          context = #context{body = pipes_util:wrap(fun to_list_fun/2), 
                             state = []}}.

to_list_fun(end_data, List) ->
    {[{data, lists:reverse(List)}, end_data], [], true};
to_list_fun({data, In}, List) ->
    {[], [In|List], false}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% fan out
fan_out(Outs) ->
    #pipe{name = fan_out,
          outputs = Outs,
          context = #context{body=fun fan_out_fun/2, state=Outs}}.

fan_out_fun([{stdin, In}], #context{state=Outs}=State) ->
    Out = [{X, [In]} || X <- Outs],
    Fin = (In==end_data),
    {Out, State#context{finished=Fin}}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% count
count() ->
    fold(fun(_, N)-> N+1 end, 0).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% grep
grep(Expr) ->
    partition(fun(X) -> regexp:first_match(X, Expr) =/= nomatch end).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% zip
zip() ->
    zip([in1, in2]).

zip(Inputs) ->
    zipwith(fun(L) -> list_to_tuple(L) end, Inputs).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% zipwith(Fun)
zipwith(Fun) ->
    zipwith(Fun, [in1, in2]).

zipwith(Fun, Inputs) ->
    #pipe{name = zipwith,
          inputs = Inputs,
          context = #context{inputs = Inputs,
                             body = fun zipwith_fun/2,
                             state = Fun}}.

zipwith_fun(Inputs, #context{state=Fun}=State) ->
    case lists:keysearch(end_data, 2, Inputs) of
        false ->
            L = [X || {_, {data, X}}<-Inputs],
            {[{stdout, [{data, Fun(L)}]}], State};
        _ ->
            {[{stdout, [end_data]}], State#context{finished=true}}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% unzip
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% unzipwith

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% all(Fun)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% any(Fun)

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% append
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% takewhile
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% dropwhile

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% sort
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% merge

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% partition
partition(Fun) ->
    #pipe{name = partition, 
          outputs=[stdout, nomatch],
          context = #context{body = fun partition_fun/2, 
                             state = Fun}}.

partition_fun([{stdin, end_data}], State) ->
    {[{stdout, [end_data]}, {nomatch, [end_data]}], State#context{finished=true}};
partition_fun([{stdin, {data, In}}], #context{state=Fun}=State) ->
    case Fun(In) of
        true ->
            {[{stdout, [{data, In}]}], State};
        false ->
            {[{nomatch, [{data, In}]}], State}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% reverse

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% split
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% splitwith
