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

% Boot
register() -> 
    {ok, _} = application:ensure_all_started(prometheus),
    ok = seven_state_classic_b_gone_prometheus:declare_metrics(),
    ok = rabbit_registry:register(channel_interceptor, ?INTERCEPTOR, ?MODULE),
    ok = rabbit_registry:register(runtime_parameter, ?PARAMETER, ?MODULE).

unregister() ->
    ok = seven_state_classic_b_gone_prometheus:undeclare_metrics(),
    ok = rabbit_registry:unregister(runtime_parameter, ?PARAMETER),
    ok = rabbit_registry:unregister(channel_interceptor, ?INTERCEPTOR).


init(Channel) ->
    VHostName = rabbit_channel:get_vhost(Channel),
    VHostData = rabbit_vhost:info(VHostName),
    Dqt = case vhost:get_default_queue_type(VHostData) of
        undefined ->
            rabbit_queue_type:default() ;
        DefaultQueueType ->
            rabbit_queue_type:discover(DefaultQueueType)    
    end,
    #channelRecord{
        vhost = VHostName,
        dqt = Dqt,
        user = rabbit_channel:get_user(Channel)
    }.

compute_actual_enforcement_mode(VHost) ->
    case enforcement_mode() of 
        enforce -> 
            case parse_mode(rabbit_runtime_parameters:value(VHost, ?PARAMETER, ?PARAMETER_NAME)) of 
                log -> log ;
                _ -> enforce
            end ;
        _ -> log
    end.
    
validate(_VHost, ?PARAMETER, ?PARAMETER_NAME, <<"enforce">>, _User) -> ok ;
validate(_VHost, ?PARAMETER, ?PARAMETER_NAME, <<"log">>, _User) -> ok ;
validate(_, _, Name, _, _) ->
    {error, "unrecognised parameter ~ts", [Name]}.

notify(_VHost, _Component, _Name, _Term, _User) -> ok.
notify_clear(_VHost, _Component, _Name, _User) -> ok.

parse_mode(<<"enforce">>) -> enforce ;
parse_mode(<<"log">>) -> log ;
parse_mode(not_found) -> not_found.

description() -> [{description, ?DESC}].

applies_to()  -> ['queue.declare'].

intercept(#'queue.declare'{arguments = Args, queue = QueueName, passive = Passive, exclusive = Exclusive} = Method, Content, _State = #channelRecord{vhost = VHost, dqt = Dqt, user = User}) ->
    EnforceMode = compute_actual_enforcement_mode(VHost),
    QTypeArg0 = rabbit_misc:table_lookup(Args, <<"x-queue-type">>),
    FinalQType = case QTypeArg0 of 
        undefined -> Dqt;
        {_Type, QTypeArg} -> rabbit_queue_type:discover(QTypeArg)
    end,
    ok = log_intercept(VHost, User, FinalQType, QueueName, Args, Passive, Exclusive),
    case early_exit_passive(Passive) of
        true ->
            passthrough(Exclusive, FinalQType, QueueName, User, VHost, fun log_early_exit_passive/5, Method, Content) ;
        false ->
            case early_exit_type(FinalQType) orelse early_exit_exclusive(Exclusive) of
                true -> 
                    passthrough(Exclusive, FinalQType, QueueName, User, VHost, fun log_early_exit_type/5, Method, Content) ;
%%                    ok = log_early_exit_type(Exclusive, FinalQType, QueueName, User, VHost),
%%                    {Method, Content} ;
                _ ->
                    case disable_existing_queues() =/= true andalso rabbit_amqqueue:exists(rabbit_misc:r(VHost, queue, QueueName)) of
                        true ->
                            passthrough(Exclusive, FinalQType, QueueName, User, VHost, fun log_existing_queue/5, Method, Content) ;
%%                            ok = log_existing_queue(FinalQType, QueueName, User, VHost),
%%                            {Method, Content} ;
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
                                            ?LOG(debug, "Found prospective effective policy ~p - is HA Policy: ~p", [PolDefinition, IsHaPolicy]),
                                            IsHaPolicy
                                    end,
                                    case HaPolicy of
                                        skip -> 
                                            enforce(EnforceMode, FinalQType, QueueName, User, VHost, all_classic_queues_disabled, Method, Content) ;
                                        false ->
                                            passthrough(Exclusive, FinalQType, QueueName, User, VHost, fun log_enforcement_none/5, Method, Content) ;
%%                                            ok = log_enforcement_none(VHost, User, FinalQType, QueueName),
%%                                            {Method, Content} ;
                                        true ->
                                            enforce(EnforceMode, FinalQType, QueueName, User, VHost, classic_mirrored_queues_disabled, Method, Content)
                                    end
                            end
                    end
            end
    end;

% Passthrough for everything we do not match previously without touching it
intercept(Method, Content, _) -> {Method, Content}.

passthrough(Exclusive, QueueType, QueueName, User, VHost, LogFunc, Method, Content) ->
    ok = LogFunc(Exclusive, QueueType, QueueName, User, VHost),
    {Method, Content}.

enforce(EnforceMode, QueueType, QueueName, User, VHost, Reason, Method, Content) ->
    ok = seven_state_classic_b_gone_prometheus:count(VHost, EnforceMode),
    ?LOG(log_level(EnforceMode), "~ts User '~p' declared queue '~ts' (~p) in vhost '~p' - reason '~ts'", [new_queue_log_message(EnforceMode), User, QueueName, QueueType, VHost, translate(Reason)]),
    enforce(EnforceMode, Method, Content).

enforce(enforce, _, _) -> 
    rabbit_misc:amqp_error(
        'precondition_failed',
        new_queue_amqp_error_text(),
        [],
        'queue.declare') ;
enforce(_, Method, Content) -> {Method, Content}.

is_ha_definition(undefined) -> false ;
is_ha_definition(PolDefinition) ->
    case proplists:get_value(<<"ha-mode">>, PolDefinition, undefined) of
        <<"all">> -> true ;
        <<"nodes">> -> true ;
        <<"exactly">> -> true;
        _ -> false
    end.

matches(P, QName) ->
    Pattern = proplists:get_value(pattern, P),
    ApplyTo = proplists:get_value('apply-to', P, <<"all">>),
    lists:member(ApplyTo, [<<"all">>, <<"queues">>, <<"classic_queues">>])
        andalso re:run(QName, Pattern, [{capture, none}]) =:= match.

winning_definition(VHost, QName) ->
    Matching = [P || P <- rabbit_policy:list(VHost), matches(P, QName)],
    ?LOG(debug, "All policies ~p", [Matching]),
    ?LOG(debug, "Sorted policies ~p", [rabbit_policy:sort_by_priority(Matching)]),
    W = case rabbit_policy:sort_by_priority(Matching) of
        []        -> undefined;
        M         -> lists:last(M)
    end,
    ?LOG(debug, "Top policy ~p", [W]),
    MatchingOp = [P || P <- rabbit_policy:list_op(VHost), matches(P, QName)],
    WOp = case rabbit_policy:sort_by_priority(MatchingOp) of
        []        -> undefined;
        M1        -> lists:last(M1)
    end,
    rabbit_policy:merge_operator_definitions(W, WOp).

-spec early_exit_type(atom()) -> (boolean()).
early_exit_type(rabbit_classic_queue) -> false;
early_exit_type(_) -> true.

early_exit_passive(true) ->
    always_let_passive_declare();
early_exit_passive(_) -> false.

early_exit_exclusive(true) ->
    disable_exclusive_queues() =:= false;
early_exit_exclusive(_) -> false.

log_early_exit_passive(_, QueueType, QueueName, User, VHost) ->
    ok = seven_state_classic_b_gone_prometheus:count(VHost, passive),
    log_early_exit("Passive declare:", QueueType, QueueName, User, VHost).

log_early_exit_type(true, QueueType, QueueName, User, VHost) ->
    ok = seven_state_classic_b_gone_prometheus:count(VHost, exclusive),
    log_early_exit("Allowed Queue Type (exclusive):", QueueType, QueueName, User, VHost);
log_early_exit_type(false, QueueType, QueueName, User, VHost) ->
    ok = seven_state_classic_b_gone_prometheus:count(VHost, type),
    log_early_exit("Allowed Queue Type:", QueueType, QueueName, User, VHost).

log_existing_queue(_, QueueType, QueueName, User, VHost) ->
    ok = seven_state_classic_b_gone_prometheus:count(VHost, existing),
    log_early_exit("Existing queue:", QueueType, QueueName, User, VHost).

log_enforcement_none(_, QueueType, QueueName, User, VHost) ->
    ok = seven_state_classic_b_gone_prometheus:count(VHost, non_mirrored),
    log_early_exit("Allowed Classic Non-Mirrored Queue:", QueueType, QueueName, User, VHost).

log_early_exit(Text, QueueType, QueueName, User, VHost) ->
    ?LOG(debug, "~p User '~p' declared queue '~p' (~p) in vhost '~p'", [Text, User, QueueName, QueueType, VHost]).

log_intercept(VHost, User, QueueType, QueueName, Args, Passive, Exclusive) ->
    ?DBG("Entering intercept for Virtual Host '~p', User '~p'~p declaring '~p' of type~p '~p'. Extra queue args: ~p", [
        VHost,
        User,
        passive_text(Passive),
        QueueName,
        exclusive_text(Exclusive),
        QueueType,
        Args
        ]),
    ok = seven_state_classic_b_gone_prometheus:count(VHost, seen).

passive_text(true) -> " passive";
passive_text(_) -> "".

exclusive_text(true) -> " exclusive";
exclusive_text(_) -> "".

log_level(enforce) ->
    application:get_env(?APP, enforce_log_level, error);
log_level(_) ->
    application:get_env(?APP, log_log_level, info).

always_let_passive_declare() ->
    application:get_env(?APP, passive, true).

disable_existing_queues() ->
    application:get_env(?APP, existing, false).

disable_exclusive_queues() ->
    application:get_env(?APP, exclusive, false).

enforcement_mode() ->
    application:get_env(?APP, mode, log).

new_queue_log_message(enforce) ->
    application:get_env(?APP, new_log_enforce, "New queue declare blocked by enforcement: ") ;

new_queue_log_message(_) ->
    application:get_env(?APP, new_log_log, "New queue declare flagged for enforcement: ").

new_queue_amqp_error_text() ->
    application:get_env(?APP, new_amqp_error, "New classic queues are not allowed on this cluster.").

match_policies() ->
    application:get_env(?APP, policies, true).

translate(classic_mirrored_queues_disabled) ->
    application:get_env(?APP, classic_mirrored_queues_disabled, "Classic Mirrored queues are disabled on this cluster") ;

translate(all_classic_queues_disabled) ->
    application:get_env(?APP, all_classic_queues_disabled, "All classic queues are disabled on this cluster") ;

translate(exclusive_queues_disabled) ->
    application:get_env(?APP, exclusive_queues_disabled, "Exclusive queues are disabled on this cluster").
