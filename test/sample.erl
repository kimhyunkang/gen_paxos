-module(sample).

-behaviour(gen_paxos).
-export([init/1, handle_message/2]).

init(Args) ->
    {ok, Args}.

handle_message(_Message, State) ->
    {ok, State}.
