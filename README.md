# Boomerang (WIP)

Send request which should be executed after some time.

Goal is to have 100k requests per second per instance (inserts / calls).

Should support: Sqlite, Postgresql, Cassandra/Scylla

## TODOS:

- [ ] Yaml config
- [ ] Mutli instance support
- [ ] Speed control/rate limit (multi instance)
- [ ] Latency based db load per endpoint
- [ ] Add tests
- [ ] Redis cache
- [ ] Add to load queue without needing to load (if sendAfter is now or in past, add to queue and save it with running status)

- [ ] Postgresql + Cassandra/Scylla 
- [ ] Tune databases
- [ ] Database sharding
