class MappingData:
    class SearchBody:
        Template = dict(
            exists={"query": {"bool": {"must": {"exists": {"field": "<<field>>"}}}}},
            not_exists={"query": {"bool": {"must_not": {"exists": {"field": "<<field>>"}}}}},
            match={"query": {"match": {"<<field>>": "<<value>>"}}},
        )

        @classmethod
        def update(cls, body_type, key, value):
            data = cls.__upack(cls.Template[body_type])
            while True:
                if data['key'] == key:
                    data['data'][data['key']] = value
                    break
                data = cls.__upack(data['value'])

        @classmethod
        def __upack(cls, data):
            data_key, = data
            return {'key': data_key, 'value': data[data_key], 'data': data}


if __name__ == '__main__':
    MappingData.SearchBody.update('match', 'match', {"<<field>>": {"query": "<<value>>"}})
    print(MappingData.SearchBody.Template)
