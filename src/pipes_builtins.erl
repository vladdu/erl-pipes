-module(pipes_builtins).

-compile([export_all]).

%% do nothing
null() ->
    fun('$end', _) ->
            {['$end'], ok};
       (_, State) ->
            {[], State}
    end.

%% print to the console
print(Fmt) ->
    fun(X, State) ->
            io:format(Fmt, [X]),
            {[X], State}
    end.

print() ->
    print("~p~n").

%% read binary stream from a file
binfile_in(FileName, Options) ->
    fun(_X, init) ->
            {ok, File} = file:open(FileName, [read_ahead, raw, binary | Options]),
            {[], File};
       (_X, File) ->
            case file:read(File, 1024) of
                {ok, Chunk} ->
                    {[Chunk], File};
                eof ->
                    file:close(File),
                    {['$end'], ok}
            end
    end.

%% read Erlang terms from a file
erl_file_in(FileName) ->
    {ok, Terms} = file:consult(FileName),
    list_in(Terms).

%% get input from a list
list_in([]) ->
    fun(_X, _S) ->
            {['$end'], ok}
    end;
list_in([H|T]) ->
    Fun1 = fun(_X, [H_|T_]) ->
                   {[H_], T_};
              (_X, []) ->
                   {['$end'], ok}
           end,
    fun(_X, _S) ->
            {[H], T, Fun1}
    end.

%% gathers all results into a list
list_out() ->
    fun(_X, init) ->
            {[], []};
       ('$end', List) ->
            {[List], ok};
       (X, List) ->
            {[], [X|List]}
    end.

