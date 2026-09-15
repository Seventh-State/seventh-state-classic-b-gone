%%% @author: Seventh State <contact@seventhstate.io>
%%% @copyright (C) 2025, Seventh State
%%% @doc 
%%% Prometheus export
%%% @end

-module(seven_state_classic_b_gone_prometheus).

-include_lib("seven_state_classic_b_gone.hrl").

-export([declare_metrics/0,
         undeclare_metrics/0,
         count/2]).

-behaviour(prometheus_collector).
-export([deregister_cleanup/1,
         collect_mf/2,
         collect_metrics/2]).

-import(prometheus_model_helpers, [create_mf/5,
                                   gauge_metric/2]).

-type count_status() :: seven_state_classic_b_gone_interceptor:enforce_mode() | passive | exclusive | type | existing | non_mirrored | seen.

-spec collect_mf(prometheus_registry:registry(), fun()) -> ok.
collect_mf(_Registry, _Callback) ->
    ok.

-spec collect_metrics(prometheus_metric:name(), tuple()) -> tuple().
collect_metrics(_, Val) ->
    Val.

%% Boot step mfa
-spec declare_metrics() -> ok.
declare_metrics() ->
    true = prometheus_counter:declare([{name, ?METRIC},
                            {labels, [vhost, status]},
                            {help, "Classic-b-gone queries intercepted"}]),
    ok.

-spec deregister_cleanup(prometheus_registry:registry()) -> ok.
deregister_cleanup(_) ->
    ok.

%% Boot step cleanup
-spec undeclare_metrics() -> ok.
undeclare_metrics() ->
    {true, _} = prometheus_counter:deregister(?METRIC),
    ok.

-spec count(binary(), count_status()) -> ok.
count(Vhost, Status) ->
    ok = prometheus_counter:inc(?METRIC, [Vhost, Status]).
