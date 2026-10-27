-module(clickhouse_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%% keep in sync with `clickhouse` module
-record(state, {url, user, key, pool, ssl_opts}).

all() ->
    [t_redact_ssl_password].

init_per_suite(TCConfig) ->
    {ok, Apps} = application:ensure_all_started(clickhouse),
    [{apps, Apps} | TCConfig].

end_per_suite(TCConfig) ->
    Apps = ?config(apps, TCConfig),
    lists:foreach(fun application:stop/1, lists:reverse(Apps)),
    ok.

t_redact_ssl_password(_TCConfig) ->
    {ok, Pid} = clickhouse:start_link([{ssl_opts, [{password, <<"secret">>}]}]),
    {status, Pid, {module, _GenServer}, Items} = sys:get_status(Pid),
    clickhouse:stop(Pid),
    ct:pal("~p", [Items]),
    [#state{ssl_opts = SSLOpts}] = [State
                                    || Items0 <- Items,
                                       is_list(Items0),
                                       {data, Items1} <- Items0,
                                       is_list(Items1),
                                       {"State", State} <- Items1
                                   ],
    ?assertMatch(<<"******">>, proplists:get_value(password, SSLOpts)),
    ok.
