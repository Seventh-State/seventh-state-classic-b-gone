%%% @author: Seventh State <contact@seventhstate.io>
%%% @copyright (C) 2025, Seventh State
%%% @doc 
%%% Interceptor for the Classic-b-gone project
%%% This interceptor will prevent creation of new classic queues
%%% @end

-module(seven_state_classic_b_gone_interceptor).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-include_lib("seven_state_classic_b_gone.hrl").

-behaviour(rabbit_channel_interceptor).
-export([init/1,
         description/0,
         applies_to/0,
         intercept/3]).

-behaviour(rabbit_runtime_parameter).
-export([validate/5,
         notify/5,
         notify_clear/4]).

-ifdef(TEST).
-compile(export_all).
-endif.

-rabbit_boot_step({?MODULE,
                    [{description, ?DESC},
                      {mfa,{?MODULE, register, []}},
                      {cleanup, {?MODULE, unregister, []}},
                      {requires, rabbit_registry},
                      {enables, recovery}]}).

-export([register/0, 
         unregister/0]).

-record(channelRecord, {
    vhost,
    dqt,
    user
}).

-type enforce_mode() :: log | enforce.
-type maybe_enforce_mode() :: enforce_mode() | not_found.
-type block_reasons() :: exclusive_queues_disabled | classic_mirrored_queues_disabled | all_classic_queues_disabled.
-type reasons() :: block_reasons() | allowed_queue_type | existing_queue.
-export_type([enforce_mode/0]).

%% Boot step mfa
-spec register() -> ok.
register() -> 
    {ok, _} = application:ensure_all_started(prometheus),
    ok = seven_state_classic_b_gone_prometheus:declare_metrics(),
    ok = rabbit_registry:register(channel_interceptor, ?INTERCEPTOR, ?MODULE),
    ok = rabbit_registry:register(runtime_parameter, ?PARAMETER, ?MODULE).

%% Boot step cleanup
-spec unregister() -> ok.
unregister() ->
    ok = seven_state_classic_b_gone_prometheus:undeclare_metrics(),
    ok = rabbit_registry:unregister(runtime_parameter, ?PARAMETER),
    ok = rabbit_registry:unregister(channel_interceptor, ?INTERCEPTOR).

%% Interceptor call when a new channel is created
-spec init(amqqueue:amqqueue()) -> #channelRecord{}.
init(Channel) ->
    VHostName = rabbit_channel:get_vhost(Channel),
    VHostData = rabbit_vhost:info(VHostName),
    Dqt = case vhost:get_default_queue_type(VHostData) of
        undefined ->
            rabbit_queue_type:default() ;
        DefaultQueueType ->
            rabbit_queue_type:discover(DefaultQueueType)    
    end,
    {user, User, _, _} = rabbit_channel:get_user(Channel),
    #channelRecord{
        vhost = VHostName,
        dqt = Dqt,
        user = User
    }.

%% Parameters validate
-spec validate(rabbit_types:vhost(), atom(), atom(), binary(), rabbit_types:username()) -> ok | {error, binary(), list()}.
validate(_VHost, ?PARAMETER, ?PARAMETER_NAME, <<"enforce">>, _User) -> ok ;
validate(_VHost, ?PARAMETER, ?PARAMETER_NAME, <<"log">>, _User) -> ok ;
validate(_, _, Name, _, _) ->
    {error, "unrecognised parameter ~ts", [Name]}.

%% Parameters notify
-spec notify(rabbit_types:vhost(), atom(), atom(), atom(), rabbit_types:username()) -> ok.
notify(_VHost, _Component, _Name, _Term, _User) -> ok.

%% Parameters notify clear
-spec notify_clear(rabbit_types:vhost(), atom(), atom(), rabbit_types:username()) -> ok.
notify_clear(_VHost, _Component, _Name, _User) -> ok.

%% Interceptor description
description() -> [{description, ?DESC}].

%% Interceptor applies_to
applies_to()  -> ['queue.declare'].

%% Interceptor
-spec intercept(#'queue.declare'{}, rabbit_types:content() | 'none', #channelRecord{}) -> {rabbit_types:content() | 'none', #channelRecord{}}.
intercept(#'queue.declare'{arguments = Args, queue = QueueName, passive = Passive, exclusive = Exclusive, durable = Durable} = Method, Content, _State = #channelRecord{vhost = VHost, dqt = Dqt, user = User}) ->
    EnforceMode = compute_actual_enforcement_mode(VHost),
    QTypeArg0 = rabbit_misc:table_lookup(Args, <<"x-queue-type">>),
    FinalQType = case QTypeArg0 of 
        undefined -> Dqt;
        {_Type, QTypeArg} -> rabbit_queue_type:discover(QTypeArg)
    end,
    ok = log_intercept(VHost, User, FinalQType, QueueName, Args, Passive, Exclusive, not Durable),
    case early_exit_passive(Passive) of
        true ->
            passthrough(Exclusive, not Durable, FinalQType, QueueName, User, VHost, fun log_early_exit_passive/6, Method, Content) ;
        false ->
            case early_exit_type(FinalQType) orelse early_exit_exclusive(Exclusive) orelse early_exit_transient(not Durable) of
                true -> 
                    passthrough(Exclusive, not Durable, FinalQType, QueueName, User, VHost, fun log_early_exit_type/6, Method, Content) ;
                _ ->
                    case disable_existing_queues() =/= true andalso rabbit_amqqueue:exists(rabbit_misc:r(VHost, queue, QueueName)) of
                        true ->
                            passthrough(Exclusive, not Durable, FinalQType, QueueName, User, VHost, fun log_existing_queue/6, Method, Content) ;
                        _ ->
                            case disable_exclusive_queues() andalso Exclusive of
                                true ->
                                    enforce(EnforceMode, FinalQType, QueueName, User, VHost, exclusive_queues_disabled, Method, Content) ;
                                _ ->
                                    HaPolicy = case match_policies() of 
                                        %% if we don't match on policies, we set to block the queue creation
                                        false -> skip ;
                                        %% if we do match policies, let's find out from the actual policies
                                        _ -> 
                                            PolDefinition = winning_definition(VHost, QueueName),
                                            IsHaPolicy = is_ha_definition(PolDefinition),
                                            ?DBG("Found prospective effective policy ~p - is HA Policy: ~p", [PolDefinition, IsHaPolicy]),
                                            IsHaPolicy
                                    end,
                                    case HaPolicy of
                                        skip -> 
                                            enforce(EnforceMode, FinalQType, QueueName, User, VHost, all_classic_queues_disabled, Method, Content) ;
                                        false ->
                                            passthrough(Exclusive, not Durable, FinalQType, QueueName, User, VHost, fun log_enforcement_none/6, Method, Content) ;
                                        true ->
                                            enforce(EnforceMode, FinalQType, QueueName, User, VHost, classic_mirrored_queues_disabled, Method, Content)
                                    end
                            end
                    end
            end
    end;

% Passthrough for everything we do not match previously without touching it
intercept(Method, Content, _) -> {Method, Content}.

%% Helpers

%% Log and passthrough
-spec passthrough(boolean(), boolean(), atom(), binary(), rabbit_types:username(), rabbit_types:vhost(), fun((boolean(), atom(), binary(), rabbit_types:username(), rabbit_types:vhost()) -> ok), #'queue.declare'{}, 'none') -> {rabbit_types:content() | 'none', #channelRecord{}}.
passthrough(Exclusive, Transient, QueueType, QueueName, User, VHost, LogFunc, Method, Content) ->
    ok = LogFunc(Exclusive, Transient, QueueType, QueueName, User, VHost),
    {Method, Content}.

%% Log and passthrough (in log mode) or enforce (in enforce mode)
-spec enforce(maybe_enforce_mode(), atom(), binary(), rabbit_types:username(), rabbit_types:vhost(), block_reasons(), rabbit_types:content() | 'none', #channelRecord{}) -> {rabbit_types:content() | 'none', #channelRecord{}}.
enforce(EnforceMode, QueueType, QueueName, User, VHost, Reason, Method, Content) ->
    ok = seven_state_classic_b_gone_prometheus:count(VHost, EnforceMode),
    ?LOG(log_level(EnforceMode), "~ts User '~p' declared queue '~ts' (~p) in vhost '~p' - reason '~ts'", [new_queue_log_message(EnforceMode), User, QueueName, QueueType, VHost, translate(Reason)]),
    enforce(EnforceMode, Reason, Method, Content).

-spec enforce(maybe_enforce_mode(), block_reasons(), rabbit_types:content() | 'none', #channelRecord{}) -> rabbit_types:amqp_error() | {rabbit_types:content() | 'none', #channelRecord{}}.
enforce(enforce, Reason, _, _) -> 
    rabbit_misc:amqp_error(
        'precondition_failed',
        translate_error(Reason),
        [],
        'queue.declare') ;
enforce(_, _, Method, Content) -> {Method, Content}.

-spec compute_actual_enforcement_mode(rabbit_types:vhost()) -> enforce_mode().
compute_actual_enforcement_mode(VHost) ->
    case enforcement_mode() of 
        enforce -> 
            case parse_mode(rabbit_runtime_parameters:value(VHost, ?PARAMETER, ?PARAMETER_NAME)) of 
                log -> log ;
                _ -> enforce
            end ;
        _ -> log
    end.

-spec parse_mode(binary() | atom()) -> maybe_enforce_mode().
parse_mode(<<"enforce">>) -> enforce ;
parse_mode(<<"log">>) -> log ;
parse_mode(not_found) -> not_found.

-spec is_ha_definition(proplists:proplist() | undefined) -> boolean().
is_ha_definition(undefined) -> false ;
is_ha_definition(PolDefinition) ->
    case proplists:get_value(<<"ha-mode">>, PolDefinition, undefined) of
        <<"all">> -> true ;
        <<"nodes">> -> true ;
        <<"exactly">> -> true;
        _ -> false
    end.

-spec matches(proplists:proplist(), binary()) -> boolean().
matches(P, QName) ->
    Pattern = proplists:get_value(pattern, P),
    ApplyTo = proplists:get_value('apply-to', P, <<"all">>),
    lists:member(ApplyTo, [<<"all">>, <<"queues">>, <<"classic_queues">>])
        andalso re:run(QName, Pattern, [{capture, none}]) =:= match.

-spec winning_definition(rabbit_types:vhost(), binary()) -> proplists:proplist() | undefined.
winning_definition(VHost, QName) ->
    Matching = [P || P <- rabbit_policy:list(VHost), matches(P, QName)],
    ?DBG("All policies ~p", [Matching]),
    ?DBG("Sorted policies ~p", [rabbit_policy:sort_by_priority(Matching)]),
    W = case rabbit_policy:sort_by_priority(Matching) of
        []        -> undefined;
        M         -> lists:last(M)
    end,
    ?DBG("Top policy ~p", [W]),
    MatchingOp = [P || P <- rabbit_policy:list_op(VHost), matches(P, QName)],
    WOp = case rabbit_policy:sort_by_priority(MatchingOp) of
        []        -> undefined;
        M1        -> lists:last(M1)
    end,
    rabbit_policy:merge_operator_definitions(W, WOp).

-spec early_exit_type(rabbit_queue_type:queue_type()) -> (boolean()).
early_exit_type(rabbit_classic_queue) -> false;
early_exit_type(_) -> true.

-spec early_exit_passive(boolean()) -> (boolean()).
early_exit_passive(true) ->
    always_let_passive_declare();
early_exit_passive(_) -> false.

-spec early_exit_exclusive(boolean()) -> (boolean()).
early_exit_exclusive(true) ->
    disable_exclusive_queues() =:= false;
early_exit_exclusive(_) -> false.

-spec early_exit_transient(boolean()) -> (boolean()).
early_exit_transient(true) ->
    allow_transient_queues() =:= true;
early_exit_transient(_) -> false.

%% Logging 

-spec log_early_exit_passive(boolean(), boolean(), rabbit_queue_type:queue_type(), binary(), rabbit_types:username(), rabbit_types:vhost()) -> no_return().
log_early_exit_passive(_, _, QueueType, QueueName, User, VHost) ->
    ok = seven_state_classic_b_gone_prometheus:count(VHost, passive),
    log_early_exit(passive, "", QueueType, QueueName, User, VHost).

-spec log_early_exit_type(boolean(), boolean(), rabbit_queue_type:queue_type(), binary(), rabbit_types:username(), rabbit_types:vhost()) -> no_return().
log_early_exit_type(false, false, QueueType, QueueName, User, VHost) ->
    ok = seven_state_classic_b_gone_prometheus:count(VHost, type),
    log_early_exit(allowed_queue_type, "", QueueType, QueueName, User, VHost) ;
log_early_exit_type(Exclusive, Transient, QueueType, QueueName, User, VHost) ->
    Category = case Exclusive of
        true -> exclusive ;
        _ -> transient
    end,
    ok = seven_state_classic_b_gone_prometheus:count(VHost, Category),
    log_early_exit(allowed_queue_type, lists:flatten(io_lib:format("(~ts~ts)", [transient_text(Transient), exclusive_text(Exclusive)])), QueueType, QueueName, User, VHost).

-spec log_existing_queue(boolean(), boolean(), rabbit_queue_type:queue_type(), binary(), rabbit_types:username(), rabbit_types:vhost()) -> no_return().
log_existing_queue(_, _, QueueType, QueueName, User, VHost) ->
    ok = seven_state_classic_b_gone_prometheus:count(VHost, existing),
    log_early_exit(existing_queue, "", QueueType, QueueName, User, VHost).

-spec log_enforcement_none(boolean(), boolean(), rabbit_queue_type:queue_type(), binary(), rabbit_types:username(), rabbit_types:vhost()) -> no_return().
log_enforcement_none(_, _, QueueType, QueueName, User, VHost) ->
    ok = seven_state_classic_b_gone_prometheus:count(VHost, non_mirrored),
    log_early_exit(allowed_queue_type, "(non-mirrored)", QueueType, QueueName, User, VHost).

-spec log_early_exit(block_reasons(), string(), rabbit_queue_type:queue_type(), binary(), rabbit_types:username(), rabbit_types:vhost()) -> no_return().
log_early_exit(TextKey, ExtraText, QueueType, QueueName, User, VHost) ->
    ?DBG("~ts~ts: User '~p' declared queue '~ts' (~ts) in vhost '~ts'", [translate(TextKey), ExtraText, User, QueueName, QueueType, VHost]).

-spec log_intercept(rabbit_types:vhost(), rabbit_types:username(), rabbit_queue_type:queue_type(), binary(), proplists:proplist(), boolean(), boolean(), boolean()) -> ok.
log_intercept(VHost, User, QueueType, QueueName, Args, Passive, Exclusive, Transient) ->
    ?DBG("Entering intercept for Virtual Host '~ts', User '~p'~ts declaring '~ts' of type~ts~ts '~p'. Extra queue args: ~p", [
        VHost,
        User,
        passive_text(Passive),
        QueueName,
        exclusive_text(Exclusive),
        transient_text(Transient),
        QueueType,
        Args
        ]),
    ok = seven_state_classic_b_gone_prometheus:count(VHost, seen).

%% Configuration

-spec passive_text(boolean()) -> string().
passive_text(true) -> " passive";
passive_text(_) -> "".

-spec exclusive_text(boolean()) -> string().
exclusive_text(true) -> " exclusive";
exclusive_text(_) -> "".

-spec transient_text(boolean()) -> string().
transient_text(true) -> " transient";
transient_text(_) -> "".

-spec log_level(maybe_enforce_mode()) -> logger:level().
log_level(enforce) ->
    application:get_env(?APP, enforce_log_level, error);
log_level(_) ->
    application:get_env(?APP, log_log_level, info).

-spec always_let_passive_declare() -> boolean().
always_let_passive_declare() ->
    application:get_env(?APP, passive, true).

-spec disable_existing_queues() -> boolean().
disable_existing_queues() ->
    application:get_env(?APP, existing, false).

-spec disable_exclusive_queues() -> boolean().
disable_exclusive_queues() ->
    application:get_env(?APP, exclusive, false).

-spec allow_transient_queues() -> boolean().
allow_transient_queues() -> 
    application:get_env(?APP, transient, false).

-spec enforcement_mode() -> enforce_mode().
enforcement_mode() ->
    application:get_env(?APP, mode, log).

-spec new_queue_log_message(maybe_enforce_mode()) -> string().
new_queue_log_message(enforce) ->
    application:get_env(?APP, new_queue_enforce, "New queue declare blocked by enforcement: ") ;
new_queue_log_message(_) ->
    application:get_env(?APP, new_queue_log, "New queue declare flagged for enforcement: ").

-spec translate_error(block_reasons()) -> string().
translate_error(classic_mirrored_queues_disabled) ->
    application:get_env(?APP, classic_mirrored_queues_disabled_error, "New classic mirrored queues are not allowed on this cluster.") ;
translate_error(all_classic_queues_disabled) ->
    application:get_env(?APP, all_classic_queues_disabled_error, "New classic queues are not allowed on this cluster.") ;
translate_error(exclusive_queues_disabled) ->
    application:get_env(?APP, exclusive_queues_disabled_error, "New exclusive classic queues are not allowed on this cluster.").

-spec match_policies() -> boolean().
match_policies() ->
    application:get_env(?APP, policies, true).

-spec translate(reasons()) -> string().
translate(passive) -> 
    application:get_env(?APP, passive_declare_log, "Passive declare") ;
translate(allowed_queue_type) ->
    application:get_env(?APP, allowed_queue_type_log, "Allowed queue type") ;
translate(existing_queue) ->
    application:get_env(?APP, existing_queue_log, "Allowed existing queue") ;
translate(classic_mirrored_queues_disabled) ->
    application:get_env(?APP, classic_mirrored_queues_disabled_log, "Classic mirrored queues are disabled on this cluster") ;
translate(all_classic_queues_disabled) ->
    application:get_env(?APP, all_classic_queues_disabled_log, "All classic queues are disabled on this cluster") ;
translate(exclusive_queues_disabled) ->
    application:get_env(?APP, exclusive_queues_disabled_log, "Exclusive queues are disabled on this cluster").
