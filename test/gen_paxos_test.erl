-module(gen_paxos_test).
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

basic_test_() ->
    {setup,
        fun() -> net_kernel:start([test_client, shortnames]) end,
        fun(_R) -> net_kernel:stop() end,
        fun basic/0}.

basic() ->
    [_Name, HostName] = string:tokens(atom_to_list(node()), "@"),
    Host = list_to_atom(HostName),
    ?assertNot(Host =:= nohost),
    {ok, Node1} = slave:start(Host, 'gp_test1'),
    {ok, Node2} = slave:start(Host, 'gp_test2'),
    {ok, Node3} = slave:start(Host, 'gp_test3'),
    Nodes = [Node1, Node2, Node3],
    lists:foreach(fun(Node) -> rpc:call(Node, gen_paxos, start, [gp_test, sample, [], Nodes]) end, Nodes),
    ?assertEqual(ok, rpc:call(Node1, gen_paxos, atomic_broadcast, [gp_test, hello])).
