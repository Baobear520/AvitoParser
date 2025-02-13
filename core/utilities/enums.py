import enum


class CategoryType(enum.Enum):

    VEHICLES_AND_PARTS = (1, "vehicles_and_parts")
    HOUSEHOLD_EQUIPMENT = (2, "household_equipment")
    REAL_ESTATE = (4, "real_estate")
    ELECTRONICS = (6, "electronics")

    def __init__(self, category_id, verbose_name):
        self.category_id = category_id
        self.verbose_name = verbose_name

    @classmethod
    def get_by_id(cls, category_id):
        """Find the category by its ID."""
        for category in cls:
            if category.category_id == category_id:
                return category
        raise ValueError(f"Category ID {category_id} not found.")






