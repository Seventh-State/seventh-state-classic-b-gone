%%% @author: Seventh State <contact@seventhstate.io>
%%% @copyright (C) 2025, Seventh State

-module(config_schema_SUITE).

-compile(export_all).

all() ->
    [run_snippets].

%% -------------------------------------------------------------------
%% Test suite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:run_setup_steps(Config),
    rabbit_ct_config_schema:init_schemas(seven_classic_b_gone, Config1).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [{rmq_nodename_suffix, Testcase}]),
    rabbit_ct_helpers:run_steps(Config1,
                                rabbit_ct_broker_helpers:setup_steps()
                                ++ rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    Config1 =
        rabbit_ct_helpers:run_steps(Config,
                                    rabbit_ct_client_helpers:teardown_steps()
                                    ++ rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

run_snippets(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, run_snippets1, [Config]).

run_snippets1(Config) ->
    rabbit_ct_config_schema:run_snippets(Config).
