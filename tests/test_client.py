from execelasticsearch.handler import ExecES

config_num = 0
dt_config = [dict(hosts=[{'host': "0.0.0.0"}]), dict(hosts=[{'host': "0.0.0.0"}], doc_type='tags')][config_num]
es_clients = ExecES(dt=dt_config)


index = 'exec_es_test'
hosts = ["dt"]


def es_create(data_, dex=0):
    data_ = data_[dex]
    print(es_clients.create(index, data_['id'], data_, hosts))


def es_update(data_, dex=0):
    data_ = data_[dex]
    data_['account_score'] = 10
    print(es_clients.update(index, data_['id'], data_, hosts))


def es_delete(id_):
    print(es_clients.delete(index, id_, hosts))


def es_update_or_ignore(data_, dex=0):
    data_ = data_[dex]
    print(es_clients.update_or_ignore(index, data_['id'], data_, hosts))


def es_upsert(data_, dex=0):
    data_ = data_[dex]
    print(es_clients.upsert(index, data_['id'], data_, hosts))


def es_bulk_upsert(data_):
    data_[0]['account_score'] = 10
    print(es_clients.bulk_upsert(index, data_, hosts))


def es_bulk_delete(data_):
    ids = [i['id'] for i in data_]
    print(es_clients.bulk_delete(index, ids, hosts))


def es_mget(data_, _source_includes=None):
    ids = [i['id'] for i in data_]
    print(es_clients.mget(index, ids, hosts, _source_includes=_source_includes))


def es_exists(ids: list):
    print(es_clients.exists_ids(index, ids, hosts))


search_b = es_clients.search_body
search_b.update(body_type1={"query": {"bool": {"must": {"exists": {"field": "{}"}}}}})
search_b["body_type2"] = {"query": {"bool": {"must": {"exists": {"field": "{}"}}}}}


def es_search(body_args: tuple = ('id',)):
    print(search_b.body_type_list)
    res = es_clients.search(index, 'exists', body_args, 'dt')
    for i in res:
        print(i)


def es_original_func():
    print(es_clients['dt'].ping())


if __name__ == '__main__':
    es_original_func()
