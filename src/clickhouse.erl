-module(clickhouse).

-behaviour(gen_server).

%% API.
-export([ start_link/0
        , start_link/1
        , stop/1
        ]).

-export ([ insert/3
         , status/1
         ]).

%% gen_server.
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, {url, user, key, pool}).

%% API.
-spec start_link() -> {ok, pid()}.
start_link() ->
    start_link([]).

start_link(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []).

stop(Pid) ->
    gen_server:stop(Pid).

insert(Pid, SQL, Opts) ->
    gen_server:call(Pid, {insert, SQL, Opts}).

status(Pid) ->
    gen_server:call(Pid, status).

%% gen_server.
init([Opts]) ->
    State = #state{url = proplists:get_value(url, Opts, "http://127.0.0.1:8123"),
                   user = proplists:get_value(user, Opts, "default"),
                   key =  proplists:get_value(key, Opts, "123456"),
                   pool = proplists:get_value(pool, Opts, default)},
    {ok, State}.

handle_call({insert, SQL, _Opts}, _From, State = #state{url = Url, user = User, key =  Key, pool = Pool}) ->
    Reply = query(Pool, Url, User, Key, SQL),
    {reply, Reply, State};

handle_call(status, _From, State = #state{url = Url, user = User, key =  Key, pool = Pool}) ->
    case query(Pool, Url, User, Key, <<"SELECT 1">>) of
        {ok, _, _} -> {reply, true, State};
        _ -> {reply, false, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

query(Pool, Url, User, Key, SQL) ->
    Headers = [{<<"X-ClickHouse-User">>, User},
               {<<"X-ClickHouse-Key">>, Key}],
    Options = [{pool, Pool},
               {connect_timeout, 10000},
               {recv_timeout, 30000},
               {follow_redirectm, true},
               {max_redirect, 5},
               with_body],
    case hackney:request(post, Url, Headers, SQL, Options) of
        {ok, StatusCode, _Headers, ResponseBody}
          when StatusCode =:= 200 orelse StatusCode =:= 204 ->
            {ok, StatusCode, ResponseBody};
        {ok, StatusCode, _Headers, ResponseBody} ->
            {error, {StatusCode, ResponseBody}};
        {error, Reason} ->
            {error, Reason}
    end.

