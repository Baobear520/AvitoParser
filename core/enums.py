import enum


class CategoryType(enum.Enum):

    VEHICLES_AND_PARTS = (1, "Авто/мото")
    HOUSEHOLD_EQUIPMENT = (2, "Бытовая техника/Домашнее оборудование")
    REAL_ESTATE = (4, "Недвижимость")
    ELECTRONICS = (6, "Электроника")

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





