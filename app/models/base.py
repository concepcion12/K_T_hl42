"""SQLAlchemy base declarative class and metadata helpers."""

from sqlalchemy.orm import DeclarativeBase, declared_attr


class Base(DeclarativeBase):
    """Base class with automatic table naming and repr."""

    @declared_attr.directive
    def __tablename__(cls) -> str:  # type: ignore[override]
        return cls.__name__.lower()

    def __repr__(self) -> str:
        primary_keys = [column.key for column in self.__mapper__.primary_key]  # type: ignore[attr-defined]
        attrs = [f"{key}={getattr(self, key)!r}" for key in primary_keys]
        return f"<{self.__class__.__name__} {' '.join(attrs)}>"

