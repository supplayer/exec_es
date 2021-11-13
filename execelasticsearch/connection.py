from elasticsearch import Elasticsearch, RequestsHttpConnection


class Clients:
    def __init__(self, **kwargs):
        self.defult_args = dict(
            hosts=None,
            port=9200,
            use_ssl=False,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            timeout=10,
            doc_type=None,
            retry_on_timeout=True
        )
        self.connect = {k: Elasticsearch(**{**self.defult_args, **v}) for k, v in kwargs.items()}

    def __getitem__(self, item) -> Elasticsearch:
        return self.connect[item]
