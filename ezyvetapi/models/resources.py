from ezyvetapi.main import EzyVetApi


from datetime import datetime
from typing import Dict, List

import numpy as np
import pandas as pd
from cbcdb import DBManager

from ezyvetapi.models.model import Model


class Resources(Model):

    def __init__(self, location_id, db=None):
        super().__init__(location_id, db)

    def get_resources(self):
        pass