from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):  # type: ignore
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)


class Location(Base):
    __tablename__ = "locations"
    address: Mapped[str] = mapped_column(unique=True, nullable=False)
    phone: Mapped[str] = mapped_column(unique=True, nullable=True)
    # Связь с таблицей LocationProduct
    location_products = relationship("LocationProduct", back_populates="location")

    def __repr__(self) -> str:
        return f"<Location(id={self.id}, address={self.address})>"


class Product(Base):
    __tablename__ = "products"
    name: Mapped[str] = mapped_column(unique=True, nullable=False)
    # Связь с таблицей LocationProduct
    location_products = relationship("LocationProduct", back_populates="product")

    def __repr__(self) -> str:
        return f"<Product(id={self.id}, name={self.name})>"


class LocationProduct(Base):
    __tablename__ = "location_products"
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"), nullable=False)
    location_id: Mapped[int] = mapped_column(
        ForeignKey("locations.id"), nullable=False
    )
    price: Mapped[int] = mapped_column(nullable=False)

    product = relationship("Product", back_populates="location_products")
    location = relationship("Location", back_populates="location_products")

    __table_args__ = (
        UniqueConstraint("product_id", "location_id", name="uix_product_Location"),
    )

    def __repr__(self) -> str:
        return (
            f"<LocationProduct(location_address={self.location.address}, "
            f"product_name={self.product.name}, price={self.price})>"
        )
