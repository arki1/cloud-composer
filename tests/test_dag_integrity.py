import unittest
import os
import time
from airflow.models import DagBag

DAG_FOLDER = os.path.join(os.path.dirname(__file__), "..", "dags")

class TestDagIntegrity(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag(
            dag_folder=DAG_FOLDER,
            include_examples=False)

    def test_dags_are_parsed(self):
        self.assertFalse(
            len(self.dagbag.import_errors),
            'DAG import failed with %d errors' % len(self.dagbag.import_errors)
        )
