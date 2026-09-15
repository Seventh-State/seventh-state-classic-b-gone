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

collect_mf(_Registry, _Callback) ->
    ok.

collect_metrics(_, Val) ->
    Val.

declare_metrics() ->
    true = prometheus_counter:declare([{name, ?METRIC},
                            {labels, [vhost, status]},
                            {help, "Classic-b-gone queries intercepted"}]),
    ok.

undeclare_metrics() ->
    {true, _} = prometheus_counter:deregister(?METRIC),
    ok.

count(Vhost, Status) ->
    ok = prometheus_counter:inc(?METRIC, [Vhost, Status]).

deregister_cleanup(_) ->
    ok.
