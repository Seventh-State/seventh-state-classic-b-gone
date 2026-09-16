%%% @author: Seventh State <contact@seventhstate.io>
%%% @copyright (C) 2025, Seventh State

-module(seven_state_classic_b_gone_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

all() ->
    [{group, clustered_3},
     {group, clustered_1},
     {group, clustered_3_enforce},
     {group, clustered_1_enforce},
     {group, clustered_1_enforce_nopolicy},
     {group, clustered_1_enforce_transient}].

groups() ->
    [{clustered_3, [], [configuration_test_3, cluster_test]},
     {clustered_1, [], [configuration_test_1, single_node_test]},
     {clustered_3_enforce, [], [configuration_test_3_enforce, cluster_test, cluster_test_existing]},
     {clustered_1_enforce, [], [configuration_test_1_enforce, single_node_test]},
     {clustered_1_enforce_nopolicy, [], [configuration_test_1_enforce_nopolicy, single_node_test]},
     {clustered_1_enforce_transient, [], [configuration_test_1_enforce_transient, cluster_test_transient]}].

%% -------------------------------------------------------------------
%% Test suite setup/teardown
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(clustered_3 = Group, Config) ->
    init_per_group(Group, Config, 3);
init_per_group(clustered_3_enforce = Group, Config) ->
    init_per_group(Group, Config, 3);
init_per_group(Group, Config) ->
    init_per_group(Group, Config, 1).

init_per_group(Group, Config, NodesCount) ->
    inets:start(),
    Config0 =
        rabbit_ct_helpers:set_config(Config,
                                     [{rmq_nodes_count, NodesCount},
                                      {rmq_nodename_suffix, Group},
                                      {net_ticktime, 3},
                                      {metadata_store, khepri}]),
    AdditionalConfig = case Group of 
        clustered_1_enforce -> [{seven_state_classic_b_gone, [
                    {mode, enforce},
                    {log_log_level, warning},
                    {passive, false},
                    {exclusive, true},
                    {new_queue_enforce, "I am a log enforce ~p"},
                    {new_queue_log, "I am a log log ~s"},
                    {classic_mirrored_queues_disabled_error, "I am ERROR"},
                    {exclusive_queues_disabled_error, "ERROR am I"}
                ]}] ;
        clustered_1_enforce_transient -> [{seven_state_classic_b_gone, [
                    {mode, enforce},
                    {log_log_level, warning},
                    {transient, true}
                ]}] ;
        clustered_1_enforce_nopolicy -> [{seven_state_classic_b_gone, [
                    {mode, enforce},
                    {log_log_level, warning},
                    {passive, false},
                    {exclusive, true},
                    {new_queue_enforce, "I am a log"},
                    {all_classic_queues_disabled_error, "am ERROR I"},
                    {policies, false}
                ]}] ;
        clustered_3_enforce -> [{rabbit, [{cluster_partition_handling, pause_minority}]},
                 {seven_state_classic_b_gone, [
                    {mode, enforce},
                    {log_log_level, debug},
                    {passive, true},
                    {exclusive, false},
                    {policies, false},
                    {existing, true}
                ]}] ;
        clustered_3 -> [{rabbit, [{cluster_partition_handling, pause_minority}]}] ;
        clustered_1 -> [{seven_state_classic_b_gone, [
                {mode, log},
                {log_log_level, info}
            ]}]
    end,
    Config1 =
        rabbit_ct_helpers:merge_app_env(Config0, AdditionalConfig),
    Config2 = rabbit_ct_helpers:run_steps(Config1,
                                [fun rabbit_ct_broker_helpers:configure_dist_proxy/1]
                                ++ rabbit_ct_broker_helpers:setup_steps()
                                ++ rabbit_ct_client_helpers:setup_steps()),
    declare_policies(Config2),
    Config2.

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
get_config(Config, Fun) -> get_config(Config, 0, Fun).
get_config(Config, Fun, Args) 
    when is_list(Args) -> get_config(Config, 0, Fun, Args);
get_config(Config, Node, Fun)
    when is_integer(Node) -> get_config(Config, Node, Fun, []).

get_config(Config, Node, Fun, Args) ->
    rabbit_ct_broker_helpers:rpc(Config, Node, seven_state_classic_b_gone_interceptor,
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
    false = get_config(Config, match_policies),
    ok.

configuration_test_1_enforce(Config) ->
    enforce = get_config(Config, enforcement_mode),
    warning = get_config(Config, log_level, [false]),
    "I am a log enforce ~p" = get_config(Config, new_queue_log_message, [enforce]),
    "I am a log log ~s" = get_config(Config, new_queue_log_message, [log]),
    "I am ERROR" = get_config(Config, translate_error, [classic_mirrored_queues_disabled]),
    "ERROR am I" = get_config(Config, translate_error, [exclusive_queues_disabled]),
    false = get_config(Config, always_let_passive_declare),
    true = get_config(Config, disable_exclusive_queues),
    true = get_config(Config, match_policies),
    ok.

configuration_test_1_enforce_nopolicy(Config) ->
    enforce = get_config(Config, enforcement_mode),
    warning = get_config(Config, log_level, [false]),
    "I am a log" = get_config(Config, new_queue_log_message, [enforce]),
    "am ERROR I" = get_config(Config, translate_error, [all_classic_queues_disabled]),
    false = get_config(Config, always_let_passive_declare),
    true = get_config(Config, disable_exclusive_queues),
    false = get_config(Config, match_policies),
    false = get_config(Config, allow_transient_queues),
    ok.

configuration_test_1_enforce_transient(Config) ->
    true = get_config(Config, allow_transient_queues),
    ok.

cluster_test(Config) ->
    ok = common_test(Config),
    ok = only_cluster_test(Config),
    ok.
single_node_test(Config) -> 
    ok = common_test(Config),
    ok.

-record(config, {
    enforce,
    pol,
    excl,
    trans
}).

common_test(Config) ->
    Node1 = nodename(Config, 0),
    NodeConfig = #config{
        enforce = enforce =:= get_config(Config, enforcement_mode),
        pol = get_config(Config, match_policies),
        excl = get_config(Config, disable_exclusive_queues),
        trans = get_config(Config, allow_transient_queues)
    },

    Queue = atom_to_binary(?FUNCTION_NAME),
    {Connection, _Channel} = open_connection_and_channel(Config, Node1),
    %% this queue has no ha policy
    declare_classic_queue(NodeConfig, Connection, <<Queue/binary, "_classic">>, false),
    %% this queue has a non-ha policy
    declare_classic_queue(NodeConfig, Connection, <<"ha-dis.", Queue/binary, "_classic">>, false),
    %% this queue has a non-ha policy
    declare_exclusive_queue(NodeConfig, Connection, <<"ha-dis.", Queue/binary, "_exclusive">>, false),
    %% except exclusive is not HA
    declare_exclusive_queue(NodeConfig, Connection, <<"ha.", Queue/binary, "_exclusive">>, true),
    %% transient has HA
    declare_transient_queue(NodeConfig, Connection, <<"ha.", Queue/binary, "_transient">>, true),
    declare_transient_queue(NodeConfig, Connection, <<"ha-dis.", Queue/binary, "_transient">>, false),

    declare_auto_delete_queue(NodeConfig, Connection, <<"ha.", Queue/binary, "_classic_ad1">>, true, [{<<"x-queue-type">>, longstr, <<"classic">>}]),
    declare_auto_delete_queue(NodeConfig, Connection, <<"ha-dis.", Queue/binary, "_classic_ad1">>, false, [{<<"x-queue-type">>, longstr, <<"classic">>}]),
    declare_auto_delete_queue(NodeConfig, Connection, <<"ha.", Queue/binary, "_classic_ad2">>, true),
    declare_auto_delete_queue(NodeConfig, Connection, <<"ha-dis.", Queue/binary, "_classic_ad2">>, false),

    declare_classic_queue(NodeConfig, Connection, <<"ha.", Queue/binary, "_classic">>, true),
    %% test with qq if the connetion is disconnected or blocked when there is a consumer connected
    %% open a new connection to test if the connection is closed when the node is in minority partition
    Connection1 = open_connection(Config, Node1),
    {ok, Channel1} = open_channel(Connection1),
    declare_quorum_queue(NodeConfig, Connection1, <<Queue/binary, "_quorum">>),
    consume(Channel1, <<Queue/binary, "_quorum">>),
    declare_quorum_queue(NodeConfig, Connection1, <<Queue/binary, "_quorum_consume">>),

    {ok, OpenNewChannel} = open_channel(Config, Node1),
    declare_classic_queue(NodeConfig, Connection1, <<Queue/binary, "_classic_recovered">>, false),
    case NodeConfig#config.enforce of
        false ->
            assert_classic_queue_works(OpenNewChannel, <<Queue/binary, "_classic_recovered">>) ;
        _ -> ok
    end,
    assert_quorum_queue_works(OpenNewChannel, <<Queue/binary, "_quorum_consume">>),

    declare_classic_queue(NodeConfig, Connection1, <<"ha-enabled">>, true),
    declare_classic_queue(NodeConfig, Connection1, <<"ha-disable">>, false),

    % All this is testing the vhost param override
    ok = rabbit_ct_broker_helpers:rpc(
       Config, 0, rabbit_runtime_parameters, set,
       [<<"/">>, <<"seven_state_classic_b_gone">>, <<"mode">>, <<"log">>, <<"acting-user">>]),

    NodeConfigLog = #config{
        enforce = false,
        pol = get_config(Config, match_policies),
        excl = get_config(Config, disable_exclusive_queues),
        trans = get_config(Config, allow_transient_queues)
    },

    declare_classic_queue(NodeConfigLog, Connection, <<"ha.", Queue/binary, "_classic_log_mode">>, true),

    ok = rabbit_ct_broker_helpers:rpc(
       Config, 0, rabbit_runtime_parameters, clear,
       [<<"/">>, <<"seven_state_classic_b_gone">>, <<"mode">>, <<"acting-user">>]),

    declare_classic_queue(NodeConfig, Connection, <<"ha.", Queue/binary, "_classic_original_mode">>, true),

    ok = rabbit_ct_broker_helpers:rpc(
       Config, 0, rabbit_runtime_parameters, set,
       [<<"/">>, <<"seven_state_classic_b_gone">>, <<"mode">>, <<"enforce">>, <<"acting-user">>]),
    
    % This should not change the original enforcement mode (if config mode is log, it will stay log)
    declare_classic_queue(NodeConfig, Connection, <<"ha.", Queue/binary, "_classic_enforce_mode">>, true),

    ok.

expect_works(declare_quorum_queue, _Config, _Ha) ->
    true;
expect_works(declare_exclusive_queue, #config{enforce = Enforce, excl = ExclDisabled}, _Ha) ->
    not (Enforce andalso ExclDisabled);
expect_works(declare_auto_delete_queue, Config, Ha) -> 
    expect_works(declare_classic_queue, Config, Ha) ;
expect_works(declare_transient_queue, #config{enforce = Enforce, pol = Pol, trans = Transient}, Ha) ->
    Transient orelse not (Enforce andalso ((Ha orelse not Pol))) ;
expect_works(declare_classic_queue, #config{enforce = Enforce, pol = Pol}, Ha) ->
    not (Enforce andalso (Ha orelse not Pol)).

cluster_test_transient(Config) -> 
    Node1 = nodename(Config, 0),
    NodeConfig = #config{
        enforce = enforce =:= get_config(Config, enforcement_mode),
        pol = get_config(Config, match_policies),
        excl = get_config(Config, disable_exclusive_queues),
        trans = get_config(Config, allow_transient_queues)
    },
    Queue = atom_to_binary(?FUNCTION_NAME),
    {Connection, _Channel} = open_connection_and_channel(Config, Node1),

    declare_transient_queue(NodeConfig, Connection, <<"ha.", Queue/binary, "_transient">>, true),
    declare_transient_queue(NodeConfig, Connection, <<"ha-dis.", Queue/binary, "_transient">>, false),
    
    ok.

cluster_test_existing(Config) ->
    Node1 = nodename(Config, 0),
    NodeConfig = #config{
        enforce = enforce =:= get_config(Config, enforcement_mode),
        pol = get_config(Config, match_policies),
        excl = get_config(Config, disable_exclusive_queues),
        trans = get_config(Config, allow_transient_queues)
    },

    Queue = atom_to_binary(?FUNCTION_NAME),
    {Connection, _Channel} = open_connection_and_channel(Config, Node1),

    %% NB: The only way to test existing queue declare is to have a cluster in enforce mode in the config,
    %% which would prevent us from creating the queue, but we can disable it by setting the vhost to log mode
    ok = rabbit_ct_broker_helpers:rpc(
       Config, 0, rabbit_runtime_parameters, set,
       [<<"/">>, <<"seven_state_classic_b_gone">>, <<"mode">>, <<"log">>, <<"acting-user">>]),
    NodeConfigLog = #config{
        enforce = false,
        pol = get_config(Config, match_policies),
        excl = get_config(Config, disable_exclusive_queues),
        trans = get_config(Config, allow_transient_queues)
    },
    declare_classic_queue(NodeConfigLog, Connection, <<"ha.", Queue/binary, "_classic_enforce_mode">>, true),
    
    ok = rabbit_ct_broker_helpers:rpc(
       Config, 0, rabbit_runtime_parameters, clear,
       [<<"/">>, <<"seven_state_classic_b_gone">>, <<"mode">>, <<"acting-user">>]),
    declare_classic_queue(NodeConfig, Connection, <<"ha.", Queue/binary, "_classic_enforce_mode">>, true),

    ok.

only_cluster_test(Config) -> 
    Queue = atom_to_binary(?FUNCTION_NAME),
    Node2Config = #config{
        enforce = enforce =:= get_config(Config, 1, enforcement_mode),
        pol = get_config(Config, 1, match_policies),
        excl = get_config(Config, 1, disable_exclusive_queues),
        trans = get_config(Config, allow_transient_queues)
    },
    Node3Config = #config{
        enforce = enforce =:= get_config(Config, 2, enforcement_mode),
        pol = get_config(Config, 2, match_policies),
        excl = get_config(Config, 2, disable_exclusive_queues),
        trans = get_config(Config, allow_transient_queues)
    },
    Node2 = nodename(Config, 1),
    Connection2 = open_connection(Config, Node2),
    Node3 = nodename(Config, 2),
    Connection3 = open_connection(Config, Node3),
    ?assert(is_pid(Connection2), "Connection to Node2 should be able to open"),
    ?assert(is_pid(Connection3), "Connection to Node3 should be able to open"),
    declare_quorum_queue(Node2Config, Connection2, <<Queue/binary, "_quorum_2">>),
    declare_quorum_queue(Node3Config, Connection3, <<Queue/binary, "_quorum_3">>),
    declare_quorum_queue(Node3Config, Connection2, <<Queue/binary, "_quorum_3">>),
    declare_quorum_queue(Node2Config, Connection3, <<Queue/binary, "_quorum_2">>),
    ok.

declare_policies(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_policy, set, [<<"/">>, <<"ha-all">>, <<"^ha.*">>, [{<<"ha-mode">>, <<"all">>}], 1, <<"queues">>, <<"acting-user">>]),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_policy, set, [<<"/">>, <<"ha-all-disable">>, <<"^ha-dis.*">>, [{<<"max-length">>, 100}], 2, <<"queues">>, <<"acting-user">>]).
%%    ok = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_policy, set, [<<"/">>, <<"unrelated">>, <<"^.*">>, [{<<"max-length-bytes">>, 1000000}], 0, <<"queues">>, <<"acting-user">>]).

declare_classic_queue(Config, Connection, Queue, Ha) ->
    declare_classic_queue(Config, Connection, Queue, Ha, []).

declare_classic_queue(Config, Connection, Queue, Ha, Args) ->
    run_declare(expect_works(?FUNCTION_NAME, Config, Ha), Connection, #'queue.declare'{queue = Queue,
                                        durable = true,
                                        auto_delete = false,
                                        arguments = Args}).

declare_transient_queue(Config, Connection, Queue, Ha) ->
    declare_transient_queue(Config, Connection, Queue, Ha, []).

declare_transient_queue(Config, Connection, Queue, Ha, Args) ->
    run_declare(expect_works(?FUNCTION_NAME, Config, Ha), Connection, #'queue.declare'{queue = Queue,
                                        durable = false,
                                        auto_delete = false,
                                        exclusive = false,
                                        arguments = Args}).

declare_exclusive_queue(Config, Connection, Queue, Ha) ->
    declare_exclusive_queue(Config, Connection, Queue, Ha, []).

declare_exclusive_queue(Config, Connection, Queue, Ha, Args) ->
    run_declare(expect_works(?FUNCTION_NAME, Config, Ha), Connection, #'queue.declare'{queue = Queue,
                                       durable = false,
                                       auto_delete = true,
                                       exclusive = true,
                                       arguments = Args}).

declare_quorum_queue(Config, Connection, Queue) ->
    declare_quorum_queue(Config, Connection, Queue, []).

declare_quorum_queue(Config, Connection, Queue, Args) ->
    % We don't want any x-queue-type, as it's injected by this function
    false = lists:keymember(<<"x-queue-type">>, 1, Args),
    run_declare(expect_works(?FUNCTION_NAME, Config, true), Connection, #'queue.declare'{queue = Queue,
                                       durable = true,
                                       auto_delete = false,
                                       arguments = Args ++ [{<<"x-queue-type">>, longstr, <<"quorum">>}]}).

declare_auto_delete_queue(Config, Connection, Queue, Ha) -> 
    declare_auto_delete_queue(Config, Connection, Queue, Ha, []).

declare_auto_delete_queue(Config, Connection, Queue, Ha, Args) ->
    run_declare(expect_works(?FUNCTION_NAME, Config, Ha), Connection, #'queue.declare'{queue = Queue,
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
