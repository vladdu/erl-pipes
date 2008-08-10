%% Distributed under the simplified BSD license. See enclosed LICENSE.TXT

-module(pipes).

-export([
         new_pipeline/0,
         add_pipe/3,
         remove_pipe/2,
         connect/3,
         disconnect/3,
         
         connect_chain/2,
         connect_chains/2,
         
         %%%%%%%%%%%%%%%
         pipe/1,
         pipe/2,
         connect/1,
         connect/2,
         %%%%%%%%%%%%%%%
         
         get_result/1
        ]).

-include("pipes.hrl").

%%% base API

-record(pipeline, {
                   pipes=[], 
                   inputs=[], 
                   outputs=[]
                  }
       ).

new_pipeline() ->
    #pipeline{}.

add_pipe(#pipeline{}=PP, #pipe{}=Pipe, Label) ->
    PP.

remove_pipe(#pipeline{}=PP, Label) ->
    PP.

connect(#pipeline{}=PP, {Src, SrcOut}, {Tgt, TgtIn}) ->
    PP.

disconnect(#pipeline{}=PP, {Src, SrcOut}, {Tgt, TgtIn}) ->
    PP.

%%% utility API
%%% use extended input format (see wiki)

connect_chain(#pipeline{}=PP, [P1, P2|T]) ->
    P1a = normalize(PP, P1, out),
    P2a = normalize(PP, P2, in),
    PP1 = connect(PP, P1a, P2a),
    connect_chain(PP1, [P2|T]);
connect_chain(#pipeline{}=PP, _) ->
    PP.

connect_chains(#pipeline{}=PP, Chains) ->
    lists:foldl(fun(C, Acc) -> connect_chain(Acc, C) end, PP).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% old API

pipe(Pipe) when is_record(Pipe, pipe) ->
    pipe:start_link(Pipe);
pipe(Pipe) when is_atom(Pipe) ->
    pipe(Pipe, []).

pipe(Pipe, Args) ->
    pipe(pipes_builtins, Pipe, Args).

pipe(Module, Pipe, Args) ->
    pipe:start_link(erlang:apply(Module, Pipe, Args)).

connect({Pid1, Name1}, {Pid2, Name2}) ->
    Pid1 ! {output, Name1, Pid2},
    Pid2 ! {input, Name2, Pid1},
    ok;
connect({Pid1, Name1}, Pid2) ->
    connect({Pid1, Name1}, {Pid2, stdin});
connect(Pid1, {Pid2, Name2}) ->
    connect({Pid1, stdout}, {Pid2, Name2});
connect(Pid1, Pid2) ->
    connect({Pid1, stdout}, {Pid2, stdin}).

connect([H|T]) ->
    connect_trunk(T, H, H).

connect_trunk([], Pid1, Pid2) ->
    {Pid1, Pid2};
connect_trunk([H|T], P1, P2) ->
    connect(P2, H),
    connect_trunk(T, P1, H).

get_result(Pipe) ->
    Pipe ! {output, stdout, self()},
    Pipe ! start,
    receive
        {data, _, {data, Data}} ->
            Data
    end.


%%%%%%%%%%%%%%%%%%

normalize(PP, P, out) ->
    P;
normalize(PP, P, in) ->
    P.