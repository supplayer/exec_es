from execelasticsearch.connection import Clients
from elasticsearch import Elasticsearch, helpers, exceptions
from json import loads, dumps


def _pack(body: str):
    return lambda *args: loads(body.format(*args))


class SearchBody:
    body_list = dict(
        exists=_pack('{{"query": {{"bool": {{"must_not": {{"exists": {{"field": "{}"}}}}}}}}}}'),
    )

    def __setitem__(self, body_type, body_args: str):
        self.body_list.update({body_type: _pack(dumps(body_args))})

    def __getitem__(self, body_args):
        return self.body_list[body_args]

    def update(self, **kwargs):
        self.body_list.update({body_type: _pack(dumps(body_args)) for body_type, body_args in kwargs.items()})


class ExecES:
    def __init__(self, **kwargs):
        self.__clients = Clients(**kwargs)
        self.__doc_type = lambda host: self.__clients[host].transport.kwargs.get('doc_type')
        self.__search_body = SearchBody()

    def __getitem__(self, item) -> Elasticsearch:
        return self.__clients[item]

    def create(self, index: str, id_, data: dict, hosts: list, doc_type=None, **kwargs):
        return {host: self.__clients[host].create(index=index, id=id_, doc_type=doc_type or self.__doc_type(host),
                                                  document=data, **kwargs) for host in set(hosts)}

    def update(self, index: str, id_, data: dict, hosts: list, doc_type=None, **kwargs):
        return {host: self.__clients[host].update(index=index, id=id_, doc_type=doc_type or self.__doc_type(host),
                                                  body={'doc': data}, **kwargs) for host in set(hosts)}

    def delete(self, index: str, id_, hosts: list, doc_type=None, **kwargs):
        return {host: self.__clients[host].delete(index=index, id=id_, doc_type=doc_type or self.__doc_type(host),
                                                  **kwargs) for host in set(hosts)}

    def update_or_ignore(self, index: str, id_, data: dict, hosts: list, doc_type=None, **kwargs):
        return {host: self.__update_or_ignore(index, id_, data, host, doc_type or self.__doc_type(host),
                                              **kwargs) for host in set(hosts)}

    def upsert(self, index: str, id_, data: dict, hosts: list, doc_type=None, **kwargs):
        return {host: self.__upsert(index, id_, data, host, doc_type or self.__doc_type(host),
                                    **kwargs) for host in set(hosts)}

    def bulk_upsert(self, index: str, data: list = None, hosts: list = None, primary='id', chunk_size=500, **kwargs):
        exists_ids = self.exists_ids(index, [i[primary] for i in data], hosts)
        return {host: dict([self.__bulk_upsert_report(k, v) for k, v in helpers.parallel_bulk(
            self.__clients[host], self.__bulk_upsert(primary, data, exists_ids[host]), index=index,
            chunk_size=chunk_size, **kwargs)]) for host in set(hosts)}

    def bulk_delete(self, index: str, ids: list = None, hosts: list = None,
                    chunk_size=500, ignore_status=(404,), **kwargs):
        return {host: dict([self.__bulk_upsert_report(k, v) for k, v in helpers.parallel_bulk(
            self.__clients[host], actions=[{'_op_type': 'delete', '_id': i} for i in ids],
            index=index, chunk_size=chunk_size, ignore_status=ignore_status, **kwargs)]) for host in set(hosts)}

    def mget(self, index: str, ids: list, hosts: list, _source_includes=None, **kwargs):
        return {host: self.__mget(index, ids, host, _source_includes, **kwargs) for host in set(hosts)}

    def exists_ids(self, index: str, ids: list, hosts: list, **kwargs):
        return {host: list(self.__mget(index, ids, host, **kwargs).keys()) for host in set(hosts)}

    def search(self, index: str, body_type, body_args: list, host: str, doc_type=None,
               scroll='2m', size=1000, _source_includes=None, **kwargs) -> iter:
        source = True if _source_includes or kwargs.get('_source_excludes') else False
        body = self.__search_body[body_type](*body_args)
        res = self.__clients[host].search(index=index, body=body, doc_type=doc_type or self.__doc_type(host),
                                          scroll=scroll, size=size, _source=source,
                                          _source_includes=_source_includes, **kwargs)
        for data in self.__scroll_data(res, host):
            yield {i['_id']: i.get('_source') for i in data}

    def __scroll_data(self, search_response, host):
        sid = search_response['_scroll_id']
        scroll_size = search_response['hits']['total']['value']
        while scroll_size > 0:
            yield search_response['hits']['hits'] if search_response else []
            search_response = self.__clients[host].scroll(scroll_id=sid, scroll='2m')
            sid = search_response['_scroll_id']
            scroll_size = len(search_response['hits']['hits'])

    def __update_or_ignore(self, index: str, id_, data: dict, host, doc_type=None, **kwargs):
        try:
            return self.__clients[host].update(index=index, id=id_, doc_type=doc_type or self.__doc_type(host),
                                               body={'doc': data}, **kwargs)
        except exceptions.NotFoundError:
            return {'document_missing_exception': {'index': index, 'id': id_, 'Error': 'document missing'}}

    def __upsert(self, index: str, id_, data: dict, host, doc_type=None, **kwargs):
        try:
            return self.__clients[host].create(index=index, id=id_, doc_type=doc_type or self.__doc_type(host),
                                               document=data, **kwargs)
        except exceptions.ConflictError:
            return self.__clients[host].update(index=index, id=id_, doc_type=doc_type or self.__doc_type(host),
                                               body={'doc': data}, **kwargs)

    @classmethod
    def __bulk_upsert(cls, primary, data, exists_ids):
        return [{**({'_op_type': 'update', 'doc': i} if str(i[primary]) in exists_ids else
                    {'_op_type': 'create', '_source': i}), **{'_id': i[primary]}} for i in data]

    @classmethod
    def __bulk_upsert_report(cls, k, v):
        res = v.get("create") or v.get('update') or v.get('delete')
        # return dict(_index=res['_index'], _id=res['_id'], result=res['result'], status=k)
        return res['_id'], res['result']

    def __mget(self, index: str, ids: list, host: str, _source_includes=None, **kwargs):
        source = True if _source_includes or kwargs.get('_source_excludes') else False
        args = dict(body={'ids': ids}, index=index, _source=source, _source_includes=_source_includes, **kwargs)
        return {i['_id']: i.get('_source') for i in self.__clients[host].mget(**args)['docs'] if i['found']}
