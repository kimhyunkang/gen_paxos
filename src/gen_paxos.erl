% Generic atomic broadcast module using paxos consensus algorithm
% 
% @author Kim HyunKang <kimhyunkang@gmail.com>
%
% for implementation guidelines, see following papers
% * Cheap Paxos (Leslie Lamport and Mike Massa, 2004)
% * Paxos Made Moderately Complex (Robbert van Renesse, 2011)
%

-module(gen_paxos).
-behaviour(gen_server).

% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

% state record used by '$cons' protocols
-record(cons_st, {name, ballot, max_pair, nodes}).

init({Name, Nodes}) ->
    case lists:member(node(), Nodes) of
        false ->
            {stop, i_am_not_a_member};
        true ->
            ConsST = #cons_st{ballot = 0, max_pair = {-1, undefined}, name = Name, nodes = lists:usort(Nodes)},
            {ok, ConsST}
    end.

scout(Parent, Phase, Name, Nodes, B, Message) ->
    case Phase of
        prepare ->
            lists:foreach(fun(N) -> erlang:send({Name, N}, {'$cons_prepare', self(), B}) end, Nodes);
        accept ->
            lists:foreach(fun(N) -> erlang:send({Name, N}, {'$cons_accept', self(), B, Message}) end, Nodes)
    end,
    NodeSet = gb_sets:from_list(Nodes),
    scout_loop(Parent, Phase, NodeSet, B, Message, gb_sets:size(NodeSet), 0, 0).

scout_loop(Parent, Phase, NodeSet, B, Message, Size, Ack, Ign) ->
    receive
        {'$commander_ack', Accepted, Node, B} ->
            case gb_sets:is_member(Node, NodeSet) of
                true ->
                    {NAck, NIgn} = 
                    if
                        Accepted ->
                            {Ack+1, Ign};
                        true ->
                            {Ack, Ign+1}
                    end,
                    NewSet = gb_sets:delete(Node, NodeSet),
                    if
                        Size < NAck*2 ->
                            erlang:send(Parent, {'$scout_accepted', Phase, B, Message});
                        Size < NIgn*2 ->
                            erlang:send(Parent, {'$scout_rejected', Phase, B, Message});
                        true ->
                            scout_loop(Parent, Phase, NewSet, B, Message, Size, NAck, NIgn)
                    end;
                false ->
                    scout_loop(Parent, Phase, NodeSet, B, Message, Size, Ack, Ign)
            end;
        _Ignore ->
            scout_loop(Parent, Phase, NodeSet, B, Message, Size, Ack, Ign)
    end.

handle_call(_Message, _From, ConsST) ->
    {reply, {error, unknown_message}, ConsST}.

handle_cast(_Message, ConsST) ->
    {noreply, ConsST}.

% phase 1a
handle_info({'$cons_propose', Message}, ConsST = #cons_st{ballot = B}) ->
    Self = self(),
    spawn(fun() -> scout(Self, prepare, ConsST#cons_st.name, ConsST#cons_st.nodes, B, Message) end),
    {noreply, ConsST#cons_st{ballot = B+1}};

% phase 1b
handle_info({'$cons_prepare', Scout, B}, ConsST = #cons_st{max_pair = MaxPair}) ->
    Pair = {B, node(Scout)},
    if
        Pair < MaxPair ->
            erlang:send(Scout, {'$cons_ack', false, node(), B}),
            {noreply, ConsST};
        true ->
            erlang:send(Scout, {'$cons_ack', true, node(), B}),
            {noreply, ConsST#cons_st{max_pair = Pair}}
    end;

% phase 2a - proposal accepted
handle_info({'$scout_accepted', B, Message}, ConsST = #cons_st{}) ->
    Self = self(),
    spawn(fun() -> scout(Self, accept, ConsST#cons_st.name, ConsST#cons_st.nodes, B, Message) end),
    {noreply, ConsST};
% phase 2a - proposal rejected
handle_info({'$scout_rejected', B, Message}, ConsST = #cons_st{}) ->
    {noreply, ConsST};

% phase 2b
handle_info({'$cons_accept', Commander, B, Message}, ConsST = #cons_st{max_pair = MaxPair}) ->
    Pair = {B, node(Commander)},
    if
        Pair < MaxPair ->
            % reject this ballot
            erlang:send(Commander, {'$cons_ack', false, node(), B}),
            {noreply, ConsST};
        true ->
            % register this message
            erlang:send(Commander, {'$cons_ack', true, node(), B}),
            {noreply, ConsST#cons_st{max_pair = Pair}}
    end;

handle_info(_Message, ConsST) ->
    {noreply, ConsST}.

terminate(_Reason, _ConsST) ->
    ok.

code_change(_OldVsn, ConsST, _Extra) ->
    {ok, ConsST}.
