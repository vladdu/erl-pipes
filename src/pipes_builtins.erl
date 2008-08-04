-module(pipes_builtins).

-compile([export_all]).

-include("pipes.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% do nothing
null() ->
    Fun = fun(State) ->
                  {[], State}
          end,
    #funstate{body=Fun}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% pass on whatever is input
identity(Ins) ->
    Fun = fun(#funstate{inputs=In}=S) ->
                  Out = lists:map(fun(Id) -> pipe:fetch_input(Id) end, In),
                  {Out, S}
          end,
    #funstate{body=Fun, inputs=Ins}.

identity() ->
    identity(stdin).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% print to the console
print(Fmt) ->
    Fun = fun(State) ->
                  X = pipe:fetch_input(stdin),            
                  io:format(Fmt, [X]),
                  {[], State}
          end,
    #funstate{body=Fun}.


print() ->
    print("~p~n").

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
    Fun = fun(#funstate{state = []}=S) ->
                  {[{stdout, [end_data]}], S};
             (#funstate{state = [H|T]}=S) ->
                  {[{stdout, [H]}], S#funstate{state=T}}
          end,
    #funstate{body=Fun, state=L}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gathers all results into a list
list_out() ->
    Fun = fun(#funstate{state=List}=State) ->
                  X = pipe:fetch_input(stdin),            
                  case X of
                      end_data ->
                          {[{stdout, lists:reverse(List)},{stdout, end_data}], State};
                      _ ->
                          {[], State#funstate{state=[X|List]}}
                  end
          end,
    #funstate{body=Fun, state=[]}.

