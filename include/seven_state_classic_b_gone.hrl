%%% @author: Seventh State <contact@seventhstate.io>
%%% @copyright (C) 2025, Seventh State
%%% @doc 
%%% Headers for the Classic-b-gone project
%%% @end

-define(DESC, "7S Classic-b-gone disables classic queues").
-define(INTERCEPTOR, <<"7S Classic-b-gone Interceptor">>).
-define(PARAMETER, <<"seven_state_classic_b_gone">>).
-define(PARAMETER_NAME, <<"mode">>).
-define(METRIC, seven_state_classic_b_gone_queue_declares_total).
-define(METRIC_INBOUND, seven_state_classic_b_gone_queue_declares_seen_total).

-define(APP, seven_state_classic_b_gone).

%% Macros for logging with module prefix
-define(LOG(Level, Fmt, Args), logger:log(Level, "[~s] " ++ Fmt, [?APP | Args], #{domain => [?APP, audit]})).

-define(DBG(Fmt, Args), ?LOG(debug, Fmt, Args)).
-define(INF(Fmt, Args), ?LOG(info, Fmt, Args)).
-define(WRN(Fmt, Args), ?LOG(warning, Fmt, Args)).
-define(ERR(Fmt, Args), ?LOG(error, Fmt, Args)).
