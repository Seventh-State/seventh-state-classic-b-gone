%%% @author: Seventh State <contact@seventhstate.io>
%%% @copyright (C) 2025, Seventh State
%%% @doc 
%%% Prometheus export
%%% @end

-module(seven_state_classic_b_gone_prometheus).

-include_lib("seven_state_classic_b_gone.hrl").

-export([declare_metrics/0,
         undeclare_metrics/0,
         count/3,
         count_inbound/1]).

-behaviour(prometheus_collector).
-export([deregister_cleanup/1,
         collect_mf/2,
         collect_metrics/2]).

-import(prometheus_model_helpers, [create_mf/5,
                                   gauge_metric/2]).

-type allow_reason() :: passive | exclusive | type | existing | non_mirrored | classic | transient.
-type enforce_reason() :: mirrored | classic | exclusive.
-export_type([enforce_reason/0]).

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
                            {labels, [vhost, outcome, reason]},
                            {help, "Total queue.declare requests classified by the classic-b-gone interceptor, by outcome and reason"}]),
    true = prometheus_counter:declare([{name, ?METRIC_INBOUND},
                            {labels, [vhost]},
                            {help, "Total queue.declare requests received by the classic-b-gone interceptor"}]),
    ok.

-spec deregister_cleanup(prometheus_registry:registry()) -> ok.
deregister_cleanup(_) ->
    ok.

%% Boot step cleanup
-spec undeclare_metrics() -> ok.
undeclare_metrics() ->
    {true, _} = prometheus_counter:deregister(?METRIC),
    {true, _} = prometheus_counter:deregister(?METRIC_INBOUND),
    ok.

-spec count_inbound(binary()) -> ok.
count_inbound(Vhost) ->
    ok = prometheus_counter:inc(?METRIC_INBOUND, [Vhost]).

-spec count(binary(), allow, allow_reason()) -> ok ;
           (binary(), seven_state_classic_b_gone_interceptor:enforce_mode(), enforce_reason()) -> ok.
count(Vhost, Outcome, Reason) ->
    ok = prometheus_counter:inc(?METRIC, [Vhost, Outcome, Reason]).
