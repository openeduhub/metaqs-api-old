from typing import Literal

class ElasticQuery:
    def __init__(self, **kwargs):
        self.attribute: str = kwargs.get("attribute", None)
        self.collection_id: str = kwargs.get("collection_id", None)
        self.index: str = kwargs.get("index", "workspace")
        self.size: int = kwargs.get("size", 10000)
        self.additional_must: dict = None

    def getBaseCondition(self) -> dict:
        must_conditions = [
            {"terms": {"type": ['ccm:io']}},
            {"terms": {"permissions.read": ['GROUP_EVERYONE']}},
            {"terms": {"properties.cm:edu_metadataset": ['mds_oeh']}},
            {"terms": {"nodeRef.storeRef.protocol": ['workspace']}},
        ]
        if self.additional_must:
            must_conditions.append(self.additional_must)

        if self.collection_id:
            must_conditions.append(
                {"bool": {
                    "should": [
                        {"match": {"collections.path": self.collection_id}},
                        {"match": {"collections.nodeRef.id": self.collection_id}},
                    ],
                    "minimum_should_match": 1
                }
                }
            )
        return {
            "bool": {
                "must": must_conditions
            }
        }

    @property
    def body(self):
        body = {}
        return body


class AggQuery(ElasticQuery):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.agg_type: Literal["terms", "missing"] = kwargs.get("agg_type", "terms")

    @property
    def body(self):
        must_condition = {
            "query": {
                "bool": {
                    "must": [
                        self.getBaseCondition(),
                    ]
                }
            }
        }
        if self.agg_type == "terms":
            agg = {"terms": {
                "field": self.attribute,
                "size": self.size
            }}
        elif self.agg_type == "missing":
            agg = {
                "missing": {
                    "field": self.attribute
                }
            }
        else:
            raise ValueError(f"agg_type: {self.agg_type} is not allowed. Use one of [\"terms\", \"missing\"]")

        body = {
            "size": 0,
            "aggs": {
                "my-agg": agg
            }
        }
        if self.index == "workspace":
            body.update(must_condition)

        return body
