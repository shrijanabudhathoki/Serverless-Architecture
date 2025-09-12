import unittest
from lambda_function import validate_row

class TestValidation(unittest.TestCase):

    def test_valid_row(self):
        """Row with all valid values should pass validation"""
        row = {
            "event_time": "2025-09-12T10:00:00Z",
            "user_id": "user123",
            "heart_rate": "75",
            "spo2": "98",
            "steps": "1000",
            "temp_c": "36.5",
            "systolic_bp": "120",
            "diastolic_bp": "80"
        }
        valid, reason = validate_row(row)
        self.assertTrue(valid)
        self.assertEqual(reason, "ok")

    def test_invalid_heart_rate(self):
        """Row with heart rate outside the allowed range should fail"""
        row = {
            "event_time": "2025-09-12T10:00:00Z",
            "user_id": "user123",
            
            "heart_rate": "200",
            "spo2": "98",
            "steps": "1000",
            "temp_c": "36.5",
            "systolic_bp": "120",
            "diastolic_bp": "80"
        }
        valid, reason = validate_row(row)
        self.assertFalse(valid)
        self.assertEqual(reason, "invalid_heart_rate")

    def test_missing_field(self):
        """Row missing a required field should fail"""
        row = {
            "event_time": "2025-09-12T10:00:00Z",
            "user_id": "user123",
            "spo2": "98",
            "steps": "1000",
            "temp_c": "36.5",
            "systolic_bp": "120",
            "diastolic_bp": "80"
        }
        valid, reason = validate_row(row)
        self.assertFalse(valid)
        self.assertEqual(reason, "missing_heart_rate")

if __name__ == "__main__":
    unittest.main()
