from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.exceptions import RuntimeException

@dataclass
class SingleStoreQuotePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass
class SingleStoreIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class SingleStoreRelation(BaseRelation):
    quote_policy: SingleStoreQuotePolicy = SingleStoreQuotePolicy()
    include_policy: SingleStoreIncludePolicy = SingleStoreIncludePolicy()
    quote_character: str = '`'

    def __post_init__(self):
        if self.database != self.schema and self.database:
            raise RuntimeException(f'Cannot set database {self.database} in singlestore!')

    def render(self):
        if self.include_policy.database and self.include_policy.schema:
            raise RuntimeException(
                "Got a singlestore relation with schema and database set to "
                "include, but only one can be set"
            )
        return super().render()
