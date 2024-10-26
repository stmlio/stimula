from abc import ABC, abstractmethod

class AbstractORM(ABC):
    """Abstract class to define a generic ORM interface for CRUD operations."""

    @abstractmethod
    def create(self, model_name: str, values: dict):
        """Create a new record."""
        raise NotImplementedError

    @abstractmethod
    def read(self, model_name: str, record_id: int):
        """Read a record by its ID."""
        raise NotImplementedError

    @abstractmethod
    def update(self, model_name: str, record_id: int, values: dict):
        """Update a record with the given ID."""
        raise NotImplementedError

    @abstractmethod
    def delete(self, model_name: str, record_id: int):
        """Delete a record by its ID."""
        raise NotImplementedError
