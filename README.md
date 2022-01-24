# exec_es


exec_es 对ElasticSearch进行了二次开发，针对多开发环境数据写入，以及bulk进行了优化。


## Installation

1.使用python包管理工具 [pip](https://pypi.org/project/exec-es/) 进行安装。

```bash
pip install exec-es
```

## Usage

```python
from execelasticsearch import ExecES, ClientConfig, MappingMethod

# MappingData.SearchBody.update will change defult Search Body Template, use before ExecES init.
MappingMethod.SearchBody.update('match', 'match', {"<<field>>": {"query": "<<value>>"}})

dt_config = ClientConfig(hosts=[{'host': "172.28.0.1"}])
dp_config = ClientConfig(hosts=[{'host': "172.28.0.2"}], doc_type='your_tags')
es_clients = ExecES(dt=dt_config, dp=dp_config)

index = 'exec_es_test'
hosts = ["dt", "dp"]


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
search_b.update(body_type1={"query": {"bool": {"must": {"exists": {"field": "<<field>>"}}}}})
search_b["body_type2"] = {"query": {"bool": {"must": {"exists": {"field": "<<field>>"}}}}}
print(search_b.body_type_list)  # show default body_type list


def es_search(body_kwargs: dict = None):
    res = es_clients.search(index, body_kwargs or {'match': dict(field='id', value=1)}, 'dt')
    for i in res:
        print(i)


def es_original_func():
    print(es_clients['dt'].ping())


if __name__ == '__main__':
    es_original_func()
```

## Contributing
使用前请做适当的测试，以确定跟您的项目完全兼容。


## License
[MIT](https://choosealicense.com/licenses/mit/)