# Boomerang (WIP)

Send request which should be executed after some time.

Goal is to have 100k requests per second per instance (inserts / calls).

Should support: Sqlite, Postgresql, Cassandra/Scylla

## TODOS:

- [ ] Yaml config
- [ ] Mutli instance support
- [ ] Speed control/rate limit (multi instance)
- [ ] Latency based db load per endpoint

- [ ] Postgresql + Cassandra/Scylla 
- [ ] Tune databases
- [ ] Database sharding
