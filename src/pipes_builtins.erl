%% Distributed under the simplified BSD license. See enclosed LICENSE.TXT

-module(pipes_builtins).

-compile([export_all]).

-include("pipes.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% do nothing
null() ->
    #pipe{name = null,
          outputs = [],
          context = #context{body=fun null_fun/2}}.

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
%% process each element
map(Fun) ->
    #pipe{name = map, 
          context = #context{body=fun map_fun/2, state=Fun}}.

map_fun([{default, end_data}], _Fun) ->
    {[{default, [end_data]}], #context{finished=true}};
map_fun([{default, {data, In}}], Fun) ->
    {[{default, [{data, Fun(In)}]}], #context{body=fun map_fun/2, state=Fun}}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% print to the console
print(Fmt) ->
    #pipe{name = print,
          outputs=[],
          context = #context{body=fun print_fun/2, state=Fmt}}.

print() ->
    print("~p~n").

print_fun([{default, X}], Fmt) ->
    io:format(Fmt, [X]),
    Fin = (X==end_data),
    {[], #context{body=fun print_fun/2, state=Fmt, finished=Fin}}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% read binary stream from a file
bcat(FileName) ->
    bcat(FileName, 1024).

bcat(FileName, ChunkSize) ->
    {ok, File} = file:open(FileName, [read_ahead, binary]),
    #pipe{name = bcat,
          inputs = [],
          context = #context{body=fun bcat_fun/2, inputs=[], state={File, ChunkSize}}}.

bcat_fun(_, {File, ChunkSize}) ->
    case file:read(File, ChunkSize) of
        {ok, Chunk} ->
            {[{default, [{data, Chunk}]}], #context{body=fun bcat_fun/2, 
                                                    state={File, ChunkSize}, 
                                                    inputs=[]}};
        _ ->
            file:close(File),
            {[{default, [end_data]}], #context{finished=true}}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% split a binary in lines
bsplit(Term) ->
    #pipe{name = bsplit,
          context = #context{body=fun bsplit_fun/2, 
                             state={Term, <<>>}}}.

bsplit_fun([{default, end_data}], {_Term, Rest}) ->
    {[{default, [{data, Rest}, end_data]}], #context{finished=true}};
bsplit_fun([{default, {data, In}}], {Term, Rest}) ->
    Bin = <<Rest/binary, In/binary>>,
    {Tokens, Rest2} = binary_tokens(Bin, Term),
    Data = [{data, T} || T<-Tokens],
    {[{default, Data}], #context{body=fun bsplit_fun/2, state={Term, Rest2}}}.

binary_tokens(Bin, Term) ->
    parse(Bin, Term, []).

find(<<>>, _Term, _N) ->
    not_found;
find(Bin, Term, N) ->
    Len = size(Term),
    case Bin of
        <<Term:Len/binary, _/binary>> ->
            N;
        <<_:8, Rest/binary>> ->
            find(Rest, Term, N+1)
    end.

parse(Bin, Term, Res) ->
    case find(Bin, Term, 0) of
        not_found ->
            {lists:reverse(Res), Bin};
        N ->
            {H, T} = split_binary(Bin, N),
            {_, T1} = split_binary(T, size(Term)),
            parse(T1, Term, [H|Res])
    end.

blines() ->
    bsplit(<<"\n">>).

bwords() ->
    bsplit(<<" ">>).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% read Erlang terms from a file
ecat(FileName) ->
    {ok, Terms} = file:consult(FileName),
    from_list(Terms).

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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% count
count() ->
    #pipe{name = count,
          context = #context{body=fun count_fun/2, state=0}}.

count_fun([{default, end_data}], N) ->
    {[{default, [{data, N}, end_data]}], #context{finished=true}};
count_fun([{default, {data, _In}}], N) ->
    {[], #context{body=fun count_fun/2, state=N+1}}.


