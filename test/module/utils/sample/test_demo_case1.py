



import pytest

class TestMyClass:
    @classmethod
    def setup_class(cls):
        # Perform setup actions for the entire test class
        print("Setup class")

    @classmethod
    def teardown_class(cls):
        # Perform teardown actions for the entire test class
        print("Teardown class")

    def setup_method(self, method):
        # Perform setup actions before each test method
        print("Setup method")

    def teardown_method(self, method):
        # Perform teardown actions after each test method
        print("Teardown method")

    def test_end_to_end_demo_case1(self):
        from pytaskqml.utils.sample.demo_case1_main import main
        main()
        

