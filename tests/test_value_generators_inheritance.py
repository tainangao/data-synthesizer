import unittest
from unittest.mock import patch

from faker import Faker

from gen_data_depricated.value_generators import non_key_value


class NonKeyValueInheritanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fake = Faker()
        self.fake.seed_instance(42)
        self.state = {
            "relationship_context": {},
            "categorical_options": {},
        }

    def test_type_column_does_not_copy_purpose_values(self) -> None:
        column = {
            "name": "loan_type",
            "type": "TEXT",
            "field_role": "categorical",
            "nullable": False,
            "primary_key": False,
            "foreign_key": None,
        }

        parent_profiles = [
            {
                "loan_purpose": "loan_purpose_a",
                "application_status": "Active",
            }
        ]

        with patch("src.gen_config.value_generators.random.random", return_value=0.0):
            value, relationship_info = non_key_value(
                fake=self.fake,
                table_name="LoanAccounts",
                col=column,
                row={},
                parent_profiles=parent_profiles,
                state=self.state,
            )

        self.assertIsNone(relationship_info)
        self.assertIn(value, {"Standard", "Premium", "Basic", "Enterprise"})
        self.assertNotIn(
            value,
            {"loan_purpose_a", "loan_purpose_b", "loan_purpose_c", "loan_purpose_d"},
        )

    def test_type_column_does_not_copy_date_values(self) -> None:
        column = {
            "name": "payment_type",
            "type": "TEXT",
            "field_role": "categorical",
            "nullable": False,
            "primary_key": False,
            "foreign_key": None,
        }

        parent_profiles = [{"last_payment_date": "2024-12-01"}]

        with patch("src.gen_config.value_generators.random.random", return_value=0.0):
            value, relationship_info = non_key_value(
                fake=self.fake,
                table_name="PaymentHistory",
                col=column,
                row={},
                parent_profiles=parent_profiles,
                state=self.state,
            )

        self.assertIsNone(relationship_info)
        self.assertIn(value, {"Standard", "Premium", "Basic", "Enterprise"})


if __name__ == "__main__":
    unittest.main()
