from execelasticsearch.handler import ExecES

es_clients = ExecES(dt=dict(hosts=[{'host': "0.0.0.0"}]))


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


def es_exists(ids: list):
    print(es_clients.exists_ids(index, ids, hosts))


def es_original_func():
    print(es_clients['dt'].ping())


if __name__ == '__main__':
    es_original_func()
