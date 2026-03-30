import unittest
import xml.etree.ElementTree as ET

from faker import Faker

from gen_data_depricated.value_generators import non_key_value


class NonKeyValueXmlTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fake = Faker()
        self.fake.seed_instance(42)
        self.state = {
            "relationship_context": {},
            "categorical_options": {},
        }

    def test_xml_semi_structured_column_generates_xml(self) -> None:
        column = {
            "name": "risk_snapshot_xml",
            "type": "XML",
            "field_role": "semi_structured",
            "nullable": True,
            "primary_key": False,
            "foreign_key": None,
        }

        value, relationship_info = non_key_value(
            fake=self.fake,
            table_name="Loans",
            col=column,
            row={},
            parent_profiles=[],
            state=self.state,
        )

        self.assertIsNone(relationship_info)
        self.assertIsInstance(value, str)

        root = ET.fromstring(value)
        self.assertEqual(root.tag, "risk_snapshot_xml")
        self.assertIsNotNone(root.find("source"))
        self.assertIsNotNone(root.find("value"))
        self.assertIsNotNone(root.find("score"))

    def test_json_semi_structured_column_still_generates_json(self) -> None:
        column = {
            "name": "risk_snapshot_json",
            "type": "JSON",
            "field_role": "semi_structured",
            "nullable": True,
            "primary_key": False,
            "foreign_key": None,
        }

        value, relationship_info = non_key_value(
            fake=self.fake,
            table_name="Loans",
            col=column,
            row={},
            parent_profiles=[],
            state=self.state,
        )

        self.assertIsNone(relationship_info)
        self.assertIsInstance(value, dict)


if __name__ == "__main__":
    unittest.main()
