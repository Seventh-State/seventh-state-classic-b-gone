%%% @author: Seventh State <contact@seventhstate.io>
%%% @copyright (C) 2025, Seventh State
%%% @doc 
%%% Interceptor for the Classic-b-gone project
%%% This interceptor will prevent creation of new classic queues
%%% @end

-module(seven_classic_b_gone_interceptor).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-include_lib("seven_classic_b_gone.hrl").

-behaviour(rabbit_channel_interceptor).

-export([init/1,
         description/0,
         applies_to/0,
         intercept/3]).

-ifdef(TEST).
-compile(export_all).
-endif.

-rabbit_boot_step({?MODULE,
                    [
                        {description, ?DESC},
                        {mfa,{rabbit_registry, register,
                                            [channel_interceptor,
                                              ?INTERCEPTOR,
                                              ?MODULE]}},
                        {cleanup, {rabbit_registry, unregister,
                                            [channel_interceptor,
                                              ?INTERCEPTOR]}},
                        {requires, rabbit_registry},
                        {enables, recovery}]}).

-record(channelRecord, {
    vhost,
    dqt,
    user
}).

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

description() -> [{description, ?DESC}].

applies_to()  -> ['queue.declare'].

intercept(#'queue.declare'{arguments = Args, queue = QueueName, passive = Passive, exclusive = Exclusive} = Method, Content, _State = #channelRecord{vhost = VHost, dqt = Dqt, user = User}) ->
    QTypeArg0 = rabbit_misc:table_lookup(Args, <<"x-queue-type">>),
    FinalQType = case QTypeArg0 of 
        undefined -> Dqt;
        {_Type, QTypeArg} -> rabbit_queue_type:discover(QTypeArg)
    end,
    log_intercept(true, VHost, User, FinalQType, Args, Content, Method),
    case early_exit_passive(Passive) of
        true ->
            log_early_exit_passive(FinalQType, QueueName, User, VHost),
            {Method, Content};
        false ->
            case early_exit_type(FinalQType) orelse early_exit_exclusive(Exclusive) of
                true -> 
                    log_early_exit_type(Exclusive, FinalQType, QueueName, User, VHost),
                    {Method, Content};
                _ ->
                    log_potential_action(FinalQType, QueueName, User, VHost),
                    case enforcement_mode() of
                        enforce ->
                            %% figure out if we have an existing queue
                            %% if we do, pass through
                            %% if we don't, block it
                            case rabbit_amqqueue:exists(rabbit_misc:r(VHost, queue, QueueName)) of
                                true ->
                                    {Method, Content};
                                _ ->
                                    log_enforcement(FinalQType, QueueName, User, VHost),
                                    rabbit_misc:amqp_error(
                                        'precondition_failed',
                                        new_queue_amqp_error_text(),
                                        [],
                                        'queue.declare')
                            end;
                        _ ->
                            {Method, Content}
                    end
            end
    end;

% Passthrough for everything we do not match previously
intercept(Method, Content, State) ->
    
    log_intercept(true, <<"/">>, "", fake_out, State, Content, Method),

    {Method, Content}.

-spec early_exit_type(atom()) -> (boolean()).
early_exit_type(rabbit_quorum_queue) -> true;
early_exit_type(rabbit_stream_queue) -> true;
early_exit_type(_) -> false.

early_exit_passive(true) ->
    always_let_passive_declare();
early_exit_passive(_) -> false.

early_exit_exclusive(true) ->
    disable_exclusive_queues() =:= false;
early_exit_exclusive(_) -> false.

log_early_exit_passive(QType, QName, User, VHost) ->
    log_early_exit("Passive declare:", QType, QName, User, VHost).

log_early_exit_type(true, QType, QName, User, VHost) ->
    log_early_exit("Allowed Queue Type (exclusive):", QType, QName, User, VHost);
log_early_exit_type(false, QType, QName, User, VHost) ->
    log_early_exit("Allowed Queue Type:", QType, QName, User, VHost).

log_early_exit(Text, QType, QName, User, VHost) ->
    ?LOG(debug, "~p User '~p' declared queue '~p' (~p) in vhost '~p'", [Text, User, QName, QType, VHost]).

log_potential_action(QType, QName, User, VHost) ->
    log_early_exit("Enforcement worthy queue detected:", QType, QName, User, VHost).

log_enforcement(QType, QName, User, VHost) ->
    log_early_exit(new_queue_log_message(), QType, QName, User, VHost).

% Logging what is happening right now
log_intercept(EnforcementState, VHostPath, User, QType, Args, Content, Method) ->
    ?LOG(log_level(EnforcementState), "Entering intercept ~ts // ~p // QType ~p // ~p // ~p // ~p", [VHostPath, User, QType, Args, Content, Method]).

log_level(true) ->
    application:get_env(?APP, enforce_log_level, error);
log_level(_) ->
    application:get_env(?APP, log_log_level, warning).

always_let_passive_declare() ->
    application:get_env(?APP, passive, true).

disable_exclusive_queues() ->
    application:get_env(?APP, exclusive, false).

enforcement_mode() ->
    application:get_env(?APP, mode, log).

new_queue_log_message() ->
    application:get_env(?APP, new_log, "New queue declare blocked by enforcement: ").

new_queue_amqp_error_text() ->
    application:get_env(?APP, new_amqp_error, "New classic queues are not allowed on this cluster.").
