from src.infra.models.public import counterparties
from src.infra.repositories.base import BaseRepository


class CounterpartiesRepository(BaseRepository):
    model = counterparties
    primary_key = "uid"
