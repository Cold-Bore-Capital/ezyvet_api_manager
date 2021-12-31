from ezyvetapi.models.model import Model


class Animals(Model):

    def __init__(self, location_id, db=None):
        super().__init__(location_id, db)
