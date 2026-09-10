%%% @author: Seventh State <contact@seventhstate.io>
%%% @copyright (C) 2025, Seventh State

-module(seven_classic_b_gone_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

all() ->
    [{group, clustered_3},
     {group, clustered_1},
     {group, clustered_3_enforce},
     {group, clustered_1_enforce}].

groups() ->
    [{clustered_3, [], [configuration_test_3, cluster_test_log]},
     {clustered_1, [], [configuration_test_1, single_node_test_log]},
     {clustered_3_enforce, [], [configuration_test_3_enforce, cluster_test_enforce]},
     {clustered_1_enforce, [], [configuration_test_1_enforce, single_node_test_enforce]}].

%% -------------------------------------------------------------------
%% Test suite setup/teardown
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(clustered_3 = Group, Config) ->
    init_per_group(Group, Config, 3, false);
init_per_group(clustered_3_enforce = Group, Config) ->
    init_per_group(Group, Config, 3, true);
init_per_group(clustered_1_enforce = Group, Config) ->
    init_per_group(Group, Config, 1, true);
init_per_group(clustered_1 = Group, Config) ->
    init_per_group(Group, Config, 1, false).

init_per_group(Group, Config, NodesCount, Enforce) ->
    inets:start(),
    Config0 =
        rabbit_ct_helpers:set_config(Config,
                                     [{rmq_nodes_count, NodesCount},
                                      {rmq_nodename_suffix, Group},
                                      {net_ticktime, 3},
                                      {metadata_store, khepri}]),
    AdditionalConfig = case Group of 
        clustered_1_enforce -> [{seven_classic_b_gone, [
                    {mode, enforce},
                    {log_log_level, warning},
                    {passive, false},
                    {exclusive, true},
                    {new_log, "I am a log"},
                    {new_amqp_error, "I am ERROR"}
                ]}] ;
        clustered_3_enforce -> [{rabbit, [{cluster_partition_handling, pause_minority}]},
                 {seven_classic_b_gone, [
                    {mode, enforce},
                    {log_log_level, debug},
                    {passive, true},
                    {exclusive, false}
                ]}] ;
        clustered_3 -> [{rabbit, [{cluster_partition_handling, pause_minority}]}] ;
        clustered_1 -> [{seven_classic_b_gone, [
                {mode, log},
                {log_log_level, info}
            ]}]
    end,
    Config1 =
        rabbit_ct_helpers:merge_app_env(Config0, AdditionalConfig),
    rabbit_ct_helpers:run_steps(Config1,
                                [fun rabbit_ct_broker_helpers:configure_dist_proxy/1]
                                ++ rabbit_ct_broker_helpers:setup_steps()
                                ++ rabbit_ct_client_helpers:setup_steps()).

end_per_group(_, Config) ->
    inets:stop(),
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_client_helpers:teardown_steps()
                                ++ rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------
get_config(Config, Fun) -> get_config(Config, Fun, []).

get_config(Config, Fun, Args) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, seven_classic_b_gone_interceptor,
                                 Fun, Args).

configuration_test_3(Config) ->
    log = get_config(Config, enforcement_mode),
    ok.

configuration_test_1(Config) ->
    log = get_config(Config, enforcement_mode),
    info = get_config(Config, log_level, [false]),
    ok.

configuration_test_3_enforce(Config) ->
    enforce = get_config(Config, enforcement_mode),
    debug = get_config(Config, log_level, [false]),
    true = get_config(Config, always_let_passive_declare),
    false = get_config(Config, disable_exclusive_queues),
    ok.

configuration_test_1_enforce(Config) ->
    enforce = get_config(Config, enforcement_mode),
    warning = get_config(Config, log_level, [false]),
    "I am a log" = get_config(Config, new_queue_log_message),
    "I am ERROR" = get_config(Config, new_queue_amqp_error_text),
    false = get_config(Config, always_let_passive_declare),
    true = get_config(Config, disable_exclusive_queues),
    ok.

cluster_test_log(Config) -> cluster_test(Config, false).

single_node_test_log(Config) -> single_node_test(Config, false).

cluster_test_enforce(Config) -> cluster_test(Config, true).

single_node_test_enforce(Config) -> single_node_test(Config, true).

cluster_test(Config, Enforce) ->
    ok = common_test(Config, Enforce),
    ok = only_cluster_test(Config, Enforce),
    ok.
single_node_test(Config, Enforce) -> 
    ok = common_test(Config, Enforce),
    ok.

common_test(Config, Enforce) ->
    Node1 = nodename(Config, 0),

    Queue = atom_to_binary(?FUNCTION_NAME),
    {Connection, Channel} = open_connection_and_channel(Config, Node1),
    declare_classic_queue(Enforce =/= true, Connection, <<Queue/binary, "_classic">>),
    declare_exclusive_queue(get_config(Config, disable_exclusive_queues) =/= true, Connection, <<Queue/binary, "_exclusive">>),
    declare_auto_delete_queue(Enforce =/= true, Connection, <<Queue/binary, "_classic_ad">>, [{<<"x-queue-type">>, longstr, <<"classic">>}]),
    %% test with qq if the connetion is disconnected or blocked when there is a consumer connected
    %% open a new connection to test if the connection is closed when the node is in minority partition
    Connection1 = open_connection(Config, Node1),
    {ok, Channel1} = open_channel(Connection1),
    declare_quorum_queue(true, Connection1, <<Queue/binary, "_quorum">>),
    consume(Channel1, <<Queue/binary, "_quorum">>),
    declare_quorum_queue(true, Connection1, <<Queue/binary, "_quorum_consume">>),

    {ok, OpenNewChannel} = open_channel(Config, Node1),
    declare_classic_queue(Enforce =/= true, Connection1, <<Queue/binary, "_classic_recovered">>),
    case Enforce of
        false ->
            assert_classic_queue_works(OpenNewChannel, <<Queue/binary, "_classic_recovered">>) ;
        _ -> ok
    end,
    assert_quorum_queue_works(OpenNewChannel, <<Queue/binary, "_quorum_consume">>),
    ok.

only_cluster_test(Config, _Enforce) -> 
    Queue = atom_to_binary(?FUNCTION_NAME),
    Node2 = nodename(Config, 1),
    Connection2 = open_connection(Config, Node2),
    Node3 = nodename(Config, 2),
    Connection3 = open_connection(Config, Node3),
    ?assert(is_pid(Connection2), "Connection to Node2 should be able to open"),
    ?assert(is_pid(Connection3), "Connection to Node3 should be able to open"),
    declare_quorum_queue(true, Connection2, <<Queue/binary, "_quorum_2">>),
    declare_quorum_queue(true, Connection3, <<Queue/binary, "_quorum_3">>),
    declare_quorum_queue(true, Connection2, <<Queue/binary, "_quorum_3">>),
    declare_quorum_queue(true, Connection3, <<Queue/binary, "_quorum_2">>),
    ok.

declare_classic_queue(Works, Connection, Queue) ->
    declare_classic_queue(Works, Connection, Queue, []).

declare_classic_queue(Works, Connection, Queue, Args) ->
    run_declare(Works, Connection, #'queue.declare'{queue = Queue,
                                        durable = true,
                                        auto_delete = false,
                                        arguments = Args}).

declare_exclusive_queue(Works, Connection, Queue) ->
    declare_exclusive_queue(Works, Connection, Queue, []).

declare_exclusive_queue(Works, Connection, Queue, Args) ->
    run_declare(Works, Connection, #'queue.declare'{queue = Queue,
                                       durable = false,
                                       auto_delete = true,
                                       exclusive = true,
                                       arguments = Args}).

declare_quorum_queue(Works, Connection, Queue) ->
    declare_quorum_queue(Works, Connection, Queue, []).

declare_quorum_queue(Works, Connection, Queue, Args) ->
    % We don't want any x-queue-type, as it's injected by this function
    false = lists:keymember(<<"x-queue-type">>, 1, Args),
    run_declare(Works, Connection, #'queue.declare'{queue = Queue,
                                       durable = true,
                                       auto_delete = false,
                                       arguments = Args ++ [{<<"x-queue-type">>, longstr, <<"quorum">>}]}).

declare_auto_delete_queue(Works, Connection, Queue) -> 
    declare_auto_delete_queue(Works, Connection, Queue, []).

declare_auto_delete_queue(Works, Connection, Queue, Args) ->
    run_declare(Works, Connection, #'queue.declare'{queue = Queue,
                                       durable = false,
                                       auto_delete = true,
                                       arguments = Args}).

run_declare(Works, Connection, Declare) ->
    {ok, Channel} = open_channel(Connection),
    case Works of
        true ->
            amqp_channel:call(Channel, Declare),
            close_channel(Channel) ;
        false ->
            ?assertExit({{shutdown, {server_initiated_close, 406, _}}, _},
            amqp_channel:call(Channel,Declare))
    end,
    ok.

consume(Channel, Queue) ->
    amqp_channel:subscribe(Channel,
                           #'basic.consume'{queue = Queue,
                                            consumer_tag = <<"test_consumer">>,
                                            no_ack = true},
                           self()),
    receive
        #'basic.consume_ok'{consumer_tag = _ConsumerTag} ->
            ok
    after 5000 ->
        ct:fail("Failed to consume from queue ~p", [Queue])
    end.

assert_queue_works(Channel, Queue) ->
    Payload = crypto:strong_rand_bytes(64),
    amqp_channel:call(Channel, #'queue.purge'{queue = Queue}),
    amqp_channel:call(Channel,
                      #'basic.publish'{exchange = <<>>,
                                       routing_key = Queue,
                                       mandatory = true,
                                       immediate = false},
                      #amqp_msg{payload = Payload}),
    true = amqp_channel:wait_for_confirms(Channel, {5, second}),
    {#'basic.get_ok'{}, #amqp_msg{payload = Payload}} =
        amqp_channel:call(Channel, #'basic.get'{queue = Queue, no_ack = true}).

assert_classic_queue_works(Channel, Queue) ->
    assert_queue_works(Channel, Queue).

assert_exclusive_queue_works(Channel, Queue) ->
    assert_queue_works(Channel, Queue).

assert_quorum_queue_works(Channel, Queue) ->
    assert_queue_works(Channel, Queue).

nodename(Config, N) ->
    Nodenames = nodenames(Config),
    Nodename = lists:nth(N + 1, Nodenames),
    Nodename.

nodenames(Config) ->
    Nodenames = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Nodenames.

open_connection(Config, Node) ->
    rabbit_ct_client_helpers:open_unmanaged_connection(Config, Node).

open_channel(Connection) ->
    {ok, Channel} = amqp_connection:open_channel(Connection),
    amqp_channel:call(Channel, #'confirm.select'{}),
    {ok, Channel}.

open_channel(Config, Node) ->
    Channel = rabbit_ct_client_helpers:open_channel(Config, Node),
    amqp_channel:call(Channel, #'confirm.select'{}),
    {ok, Channel}.

open_connection_and_channel(Config, Node, _VHost) ->
    {Connection, Channel} = rabbit_ct_client_helpers:open_connection_and_channel(Config, Node),
    amqp_channel:call(Channel, #'confirm.select'{}),
    {Connection, Channel}.

open_connection_and_channel(Config, Node) ->
    open_connection_and_channel(Config, Node, <<"/">>).

close_channel(Channel) -> amqp_channel:close(Channel).

wait_for_net_tick_timeout(_Config) ->
    timer:sleep(15000).

assert_connection_closed(Ref, Pid) ->
    receive
        {'DOWN', Ref, process, Pid, Reason} ->
            ct:pal("Connection closed: ~p~n", [Reason]),
            ?assertMatch({shutdown, {server_initiated_close, _, _}}, Reason);
        {'DOWN', _, process, _, Reason} ->
            ct:pal("Unexpected connection close: ~p~n", [Reason]),
            ?assert(false)
    after 35000 ->
        ?assertNot(is_process_alive(Pid), "Connection should be closed but is still alive")
    end.
