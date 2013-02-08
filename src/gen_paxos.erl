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

% behaviour definition
-export([behaviour_info/1]).

% API functions
-export([start/4, start_link/4, atomic_broadcast/2]).

% state record used by '$cons' protocols
-record(cons_st, {name, mod, ballot, max_pair, nodes, user_state, call_map, active}).

%%%% public APIs

start(Name, Mod, Args, Nodes) ->
    gen_server:start({local, Name}, ?MODULE, {Name, Mod, Args, Nodes}, []).

start_link(Name, Mod, Args, Nodes) ->
    gen_server:start_link({local, Name}, ?MODULE, {Name, Mod, Args, Nodes}, []).

atomic_broadcast(Name, Message) ->
    gen_server:call(Name, {'$cons_propose', Message}).

%%%% gen_server callbacks

init({Name, Mod, Args, Nodes}) ->
    case lists:member(node(), Nodes) of
        false ->
            {stop, i_am_not_a_member};
        true ->
            case Mod:init(Args) of
                {ok, State} ->
                    ConsST = #cons_st{
                                    name = Name,
                                    mod = Mod,
                                    ballot = 0,
                                    max_pair = {-1, undefined},
                                    nodes = lists:usort(Nodes),
                                    user_state = State,
                                    call_map = gb_trees:empty(),
                                    active = true},
                    {ok, ConsST};
                {stop, Reason} ->
                    {stop, Reason}
            end
    end.

scout(Parent, Phase, Name, Nodes, B, Message) ->
    case Phase of
        prepare ->
            lists:foreach(fun(N) -> erlang:send({Name, N}, {'$cons_prepare', self(), B}) end, Nodes);
        accept ->
            lists:foreach(fun(N) -> erlang:send({Name, N}, {'$cons_accept', self(), B, Message}) end, Nodes)
    end,
    NodeSet = gb_sets:from_list(Nodes),
    scout_loop(Parent, Phase, NodeSet, B, gb_sets:size(NodeSet), 0).

scout_loop(Parent, Phase, NodeSet, B, Size, Ack) ->
    receive
        {'$cons_ack', Node, {B, ThisNode}} when ThisNode == node() ->
            case gb_sets:is_member(Node, NodeSet) of
                true ->
                    NewSet = gb_sets:delete(Node, NodeSet),
                    NAck = Ack+1,
                    if
                        Size < NAck*2 ->
                            erlang:send(Parent, {'$scout_accepted', Phase, B});
                        true ->
                            scout_loop(Parent, Phase, NewSet, B, Size, NAck)
                    end;
                false ->
                    scout_loop(Parent, Phase, NodeSet, B, Size, Ack)
            end;
        {'$cons_ack', _Node, OtherPair} ->
            % an acceptor is preempted by higher ballot
            erlang:send(Parent, {'$scout_preempted', Phase, B, OtherPair});
        _Ignore ->
            scout_loop(Parent, Phase, NodeSet, B, Size, Ack)
    end.

handle_cast(_Message, ConsST) ->
    {noreply, ConsST}.

% phase 1a
handle_call({'$cons_propose', Message}, From, ConsST = #cons_st{ballot = B, call_map = CallMap}) ->
    Parent = self(),
    spawn_link(fun() -> scout(Parent, prepare, ConsST#cons_st.name, ConsST#cons_st.nodes, B, Message) end),
    NewST = ConsST#cons_st{ballot = B+1, call_map = gb_trees:insert(B, {From, Message}, CallMap)},
    {noreply, NewST};

handle_call(_Message, _From, ConsST) ->
    {reply, {error, unknown_message}, ConsST}.

% phase 1b
handle_info({'$cons_prepare', Scout, B}, ConsST = #cons_st{max_pair = MaxPair}) ->
    {MaxB, MaxLeader} = MaxPair,
    Pair = {B, node(Scout)},
    if
        B < MaxB; B == MaxB, node(Scout) > MaxLeader ->
            erlang:send(Scout, {'$cons_ack', node(), MaxPair}),
            {noreply, ConsST};
        true ->
            erlang:send(Scout, {'$cons_ack', node(), Pair}),
            {noreply, ConsST#cons_st{max_pair = Pair}}
    end;

% phase 2a - proposal accepted
handle_info({'$scout_accepted', prepare, B}, ConsST = #cons_st{call_map = CallMap}) ->
    case gb_trees:lookup(B, CallMap) of
        {value, {_From, Message}} ->
            Parent = self(),
            spawn_link(fun() -> scout(Parent, accept, ConsST#cons_st.name, ConsST#cons_st.nodes, B, Message) end);
        none ->
            ok
    end,
    {noreply, ConsST#cons_st{active = true}};
handle_info({'$scout_accepted', accept, B}, ConsST = #cons_st{call_map = CallMap}) ->
    case gb_trees:lookup(B, CallMap) of
        {value, {From, _Message}} ->
            gen_server:reply(From, ok),
            NewST = ConsST#cons_st{call_map = gb_trees:delete(B, CallMap)},
            {noreply, NewST};
        none ->
            {noreply, ConsST}
    end;
% phase 2a - proposal rejected
handle_info({'$scout_preempted', _Phase, B, {MaxB, _Leader}}, ConsST = #cons_st{call_map = CallMap}) ->
    PreemptedST = ConsST#cons_st{ballot = MaxB+1, active = false},
    case gb_trees:lookup(B, CallMap) of
        {value, {From, _Message}} ->
            gen_server:reply(From, preempted),
            {noreply, PreemptedST#cons_st{call_map = gb_trees:delete(B, CallMap)}};
        none ->
            {noreply, PreemptedST}
    end;

% phase 2b
handle_info({'$cons_accept', Commander, B, Message}, ConsST = #cons_st{max_pair = MaxPair}) ->
    {MaxB, MaxLeader} = MaxPair,
    Pair = {B, node(Commander)},
    if
        B < MaxB; B == MaxB, node(Commander) > MaxLeader ->
            % reject this ballot
            erlang:send(Commander, {'$cons_ack', node(), MaxPair}),
            {noreply, ConsST};
        true ->
            Mod = ConsST#cons_st.mod,
            UserST = ConsST#cons_st.user_state,

            % register this message
            erlang:send(Commander, {'$cons_ack', node(), Pair}),
            case Mod:handle_message(Message, UserST) of
                {ok, NewUserST} ->
                    {noreply, ConsST#cons_st{max_pair = Pair, user_state = NewUserST}};
                {stop, Reason} ->
                    {stop, Reason, ConsST}
            end
    end;

handle_info(_Message, ConsST) ->
    {noreply, ConsST}.

terminate(_Reason, _ConsST) ->
    ok.

code_change(_OldVsn, ConsST, _Extra) ->
    {ok, ConsST}.

%%%% behaviour info

behaviour_info(callbacks) ->
    [{init, 1}, {handle_message, 2}];
behaviour_info(_Type) ->
    undefined.
